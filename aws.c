#include "aws.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "util.h"
#include "debug.h"
#include "sock_util.h"
#include "w_iocp.h"
#include "http_parser.h"

#include <winsock2.h>
#include <mswsock.h>

#define NR_EVENTS 1
/* server socket file descriptor */
static SOCKET listenfd;

/* IoCompletionPort handle */
static HANDLE iocp;

enum connection_state {
	STATE_DATA_RECEIVED,
	STATE_DATA_PARTIAL_RECEIVED,
	STATE_DATA_SENT,
	STATE_DATA_PARTIAL_SENT,
	STATE_CONNECTION_CLOSED
};

/* structure acting as a connection handler */
struct connection {
	SOCKET sockfd;
	/* buffers used for receiving messages and then echoing them back */
	char recv_buffer[BUFSIZ];
	size_t recv_len;
	char send_buffer[BUFSIZ];
	WSAOVERLAPPED recv_ov;
	WSAOVERLAPPED send_ov;
	OVERLAPPED o;
	WSABUF recv_buffers[1];
	WSABUF send_buffers[1];
	size_t send_len;
	size_t bytes_sent;
	HANDLE fd;
	DWORD size;
	int stare;
	int caz;
	size_t offset;
	size_t offset_read;

	size_t nr_got;
	size_t nr_trimise;
	size_t nr_sub;

	size_t nr_total;

	char* send_aux;
	
	size_t last_bytes;
	void **buffers;
	enum connection_state state;

	struct iocb **piocb;
	struct iocb *iocb;
};

/*
 * Anonymous structure used to "confine" data regardin asynchronous accept
 * operations (handled through AcceptEx and Io Completion Ports).
 */
static struct {
	SOCKET sockfd;
	char buffer[BUFSIZ];
	DWORD len;
	OVERLAPPED ov;
} ac;

/*
 * Initialize connection structure on given socket.
 */

static struct connection *connection_create(SOCKET sockfd)
{
	struct connection *conn = malloc(sizeof(*conn));
	

	conn->sockfd = sockfd;
	conn->stare = 0;
	conn->caz = 0;
	conn->offset = 0;
	conn->recv_len = 0;
	conn->send_len = 0;
	conn->bytes_sent = 0;
	conn->nr_got = 0;
	conn->nr_sub = 0;
	conn->nr_total = 0;
	conn->nr_trimise = 0;
	conn->last_bytes = 0;
	conn->offset_read = 0;

	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);
	conn->recv_buffers[0].buf = conn->recv_buffer;
	conn->send_buffers[0].buf = conn->send_buffer;
	conn->recv_buffers[0].len = BUFSIZ;
	conn->send_buffers[0].len = BUFSIZ;

	memset(&conn->recv_ov, 0, sizeof(conn->recv_ov));
	memset(&conn->send_ov, 0, sizeof(conn->send_ov));

	memset(&conn->o, 0, sizeof(conn->o));
	return conn;
}

/*
 * Add a non bound socket to the connection. The socket is to be bound
 * by AcceptEx.
 */

static struct connection *connection_create_with_socket(void)
{
	SOCKET s;

	s = socket(PF_INET, SOCK_STREAM, 0);

	return connection_create(s);
}

/*
 * Remove connection handler.
 */

static void connection_remove(struct connection *conn)
{
	closesocket(conn->sockfd);
	free(conn);
}

/*
 * Use WSASend to asynchronously send message through socket.
 */

static void connection_schedule_socket_send(struct connection *conn, size_t len)
{
	DWORD flags;
	int rc;
	
	memset(&conn->send_ov, 0, sizeof(conn->send_ov));

	//bytes_sent = send(conn->sockfd, conn->send_buffer + conn->bytes_sent, conn->send_len - conn->bytes_sent, 0);
	
	flags = 0;
	conn->send_buffers[0].buf = conn->send_buffer + conn->bytes_sent;
	conn->send_buffers[0].len = len;
	
	rc = WSASend(conn->sockfd, conn->send_buffers,
			1,
			NULL,
			flags,
			&conn->send_ov,
			NULL);
	if (rc && (rc != SOCKET_ERROR || WSAGetLastError() != WSA_IO_PENDING))
		exit(EXIT_FAILURE);
}

/*
 * Use WSARecv to asynchronously receive message from socket.
 */

static void connection_schedule_socket_receive(struct connection *conn, size_t len)
{
	DWORD flags;
	int rc;

	memset(&conn->send_ov, 0, sizeof(conn->send_ov));

	//bytes_recv = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ - conn->recv_len, 0);
	
	flags = 0;
	conn->recv_buffers[0].buf = conn->recv_buffer + conn->recv_len;
	conn->recv_buffers[0].len = len;
	rc = WSARecv(conn->sockfd, conn->recv_buffers,
			1,
			NULL,
			&flags,
			&conn->recv_ov,
			NULL);
	if (rc && (rc != SOCKET_ERROR || WSAGetLastError() != WSA_IO_PENDING))
		exit(EXIT_FAILURE);
}

/*
 * Schedule overlapped operation for accepting a new connection.
 */

static void create_iocp_accept(void)
{
	BOOL bRet;

	memset(&ac, 0, sizeof(ac));

	/* Create simple socket for acceptance */
	ac.sockfd = socket(PF_INET, SOCK_STREAM, 0);

	/* Launch overlapped connection accept through AcceptEx. */
	bRet = AcceptEx(
			listenfd,
			ac.sockfd,
			ac.buffer,
			0,
			128,
			128,
			&ac.len,
			&ac.ov);
}

/*
 * Handle a new connection request on the server socket.
 */

static void handle_new_connection()
{
	struct connection *conn;
	char abuffer[64];
	HANDLE hRet;
	int rc;

	rc = setsockopt(
			ac.sockfd,
			SOL_SOCKET,
			SO_UPDATE_ACCEPT_CONTEXT,
			(char *) &listenfd,
			sizeof(listenfd)
		  );

	rc = get_peer_address(ac.sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		return;
	}


	/* Instantiate new connection handler. */
	conn = connection_create(ac.sockfd);

	/* Add socket to IoCompletionPort. */
	hRet = w_iocp_add_key(iocp, (HANDLE) conn->sockfd, (ULONG_PTR) conn);

	/* Schedule receive operation. */
	connection_schedule_socket_receive(conn, BUFSIZ);

	/* Use AcceptEx to schedule new connection acceptance. */
	create_iocp_accept();

}

static enum connection_state send_message(struct connection *conn, WSAOVERLAPPED *ovp)
{
	DWORD bytes_sent;
	int rc, nr_bytes;
	char abuffer[64];
	BOOL bRet;
	DWORD flags;

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		goto remove_connection;
	}

	bRet = WSAGetOverlappedResult(
			conn->sockfd,
			ovp,
			&bytes_sent,
			FALSE,
			&flags);
		
	if (bytes_sent < 0) {		/* error in communication */
		goto remove_connection;
	}

	if (bytes_sent == 0) {		/* connection closed */
		goto remove_connection;
	}
	
	conn->offset += bytes_sent;
	conn->bytes_sent += bytes_sent;
	if(conn->bytes_sent < conn->send_len){
		connection_schedule_socket_send(conn, conn->send_len - conn->bytes_sent);
		return STATE_DATA_PARTIAL_SENT;
	}
	
	conn->offset = 0;
	conn->bytes_sent = 0;
	conn->state = STATE_DATA_SENT;

	
	
	return STATE_DATA_SENT;

remove_connection:

	/* remove current connection */
	CloseHandle(conn->fd);
	connection_remove(conn);

	return STATE_CONNECTION_CLOSED;
}

static http_parser request_parser;
static char request_path[BUFSIZ];	/* storage for request_path */
/*
 * Callback is invoked by HTTP request parser when parsing request path.
 * Request path is stored in global request_path variable.
 */

static int on_path_cb(http_parser *p, const char *buf, size_t len)
{
	char path[BUFSIZ];
	
	assert(p == &request_parser);
	sscanf(buf, "%[^.]", path);
	sprintf(request_path, "%s%s.dat",AWS_DOCUMENT_ROOT, path+1);

	return 0;
}

/* Use mostly null settings except for on_path callback. */
static http_parser_settings settings_on_path = {
	/* on_message_begin */ 0,
	/* on_header_field */ 0,
	/* on_header_value */ 0,
	/* on_path */ on_path_cb,
	/* on_url */ 0,
	/* on_fragment */ 0,
	/* on_query_string */ 0,
	/* on_body */ 0,
	/* on_headers_complete */ 0,
	/* on_message_complete */ 0
};

/*
 * Receive (HTTP) request. Don't parse it, just read data in buffer
 * and print it.
 */

static enum connection_state receive_request(struct connection *conn, WSAOVERLAPPED *ovp)
{
	DWORD bytes_recv;
	size_t bytes_parsed;
	BOOL bRet;
	DWORD flags;
	char abuffer[64];
	int rc;

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		goto remove_connection;
	}

	bRet = WSAGetOverlappedResult(
			conn->sockfd,
			ovp,
			&bytes_recv,
			FALSE,
			&flags);

	
	/* In case of no bytes received, consider connection terminated. */
	if (bytes_recv == 0) {
		goto remove_connection;
	}
	
	if (bytes_recv < 0) {		/* error in communication */
		goto remove_connection;
	}

	conn->recv_len += bytes_recv;
	conn->state = STATE_DATA_RECEIVED;

	conn->recv_buffer[conn->recv_len] = 0;
	if(strcmp(conn->recv_buffer + conn->recv_len - 4, "\r\n\r\n") != 0) {
		connection_schedule_socket_receive(conn, BUFSIZ - conn->recv_len);
		return STATE_DATA_PARTIAL_RECEIVED;
	}
	
	
	
	/* init HTTP_REQUEST parser */
	http_parser_init(&request_parser, HTTP_REQUEST);

	bytes_parsed = http_parser_execute(&request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);
	if (bytes_parsed == 0) {		/* connection closed */
		goto remove_connection;
	}
	
	conn->recv_len = 0;
	//connection_schedule_socket_receive(conn);
	
	return STATE_DATA_RECEIVED;

remove_connection:
	/* close local socket */
	
	closesocket(conn->sockfd);
	/* remove current connection */
	connection_remove(conn);
	return STATE_CONNECTION_CLOSED;
}

/*
 * Send HTTP reply. Send simple message, don't care about request content.
 *
 * Socket is closed after HTTP reply.
 */

static void put_header(struct connection *conn)
{
	char buffer[BUFSIZ];

	 sprintf(buffer, "HTTP/1.1 200 OK\r\n"
		"Date: Sun, 08 May 2011 09:26:16 GMT\r\n"
		"Server: Apache/2.2.9\r\n"
		"Last-Modified: Mon, 02 Aug 2010 17:55:28 GMT\r\n"
		"Accept-Ranges: bytes\r\n"
		"Content-Length: %ld\r\n"
		"Vary: Accept-Encoding\r\n"
		"Connection: close\r\n"
		"Content-Type: text/html\r\n"
		"\r\n", conn->size);
	conn->send_len = strlen(buffer);
	memcpy(conn->send_buffer, buffer, strlen(buffer));
}

static void put_error(struct connection *conn)
{
	char buffer[BUFSIZ] = "HTTP/1.1 404 Not Found\r\n"
		"Date: Sun, 08 May 2011 09:26:16 GMT\r\n"
		"Server: Apache/2.2.9\r\n"
		"Last-Modified: Mon, 02 Aug 2010 17:55:28 GMT\r\n"
		"Accept-Ranges: bytes\r\n"
		"Content-Length: 153\r\n"
		"Vary: Accept-Encoding\r\n"
		"Connection: close\r\n"
		"Content-Type: text/html\r\n"
		"\r\n";
	conn->send_len = strlen(buffer);
	memcpy(conn->send_buffer, buffer, strlen(buffer));
}


/*
 * Handle a client request on a client connection.
 */

static void handle_client_request(struct connection *conn, WSAOVERLAPPED *ovp)
{
	enum connection_state ret_state;
	char static_prefix[BUFSIZ];
	char dynamic_prefix[BUFSIZ];
	DWORD high_size;

	ret_state = receive_request(conn, ovp);
	if(ret_state == STATE_CONNECTION_CLOSED || ret_state == STATE_DATA_PARTIAL_RECEIVED)
		return;
	sprintf(static_prefix, "%sstatic/", AWS_DOCUMENT_ROOT);
	sprintf(dynamic_prefix, "%sdynamic/", AWS_DOCUMENT_ROOT);
	/* Open the input file. */
	conn->fd = CreateFile(
     request_path,
     GENERIC_READ,
     FILE_SHARE_READ,
     NULL,
     OPEN_EXISTING,
     FILE_FLAG_OVERLAPPED,
     NULL
	);
	
	if(conn->fd == INVALID_HANDLE_VALUE) {
		conn->caz = 0;
		put_error(conn);
		connection_schedule_socket_send(conn, conn->send_len);
		return;
	}
	/* Stat the input file to obtain its size. */
	
	conn->size = GetFileSize(conn->fd, &high_size);
	conn->size += high_size << 8;
	conn->offset = 0;

	if (strncmp(request_path, static_prefix, strlen(static_prefix)) == 0) {
		conn->caz = 1;
		conn->stare = 0;
		put_header(conn);
	} else if (strncmp(request_path, dynamic_prefix, strlen(dynamic_prefix)) == 0) {
		conn->caz = 2;
		put_header(conn);
		conn->stare = 0;
	} else {
		put_error(conn);
		conn->caz = 0;
	}
	connection_schedule_socket_send(conn, conn->send_len);
}

static void send_file_aio(struct connection* conn) {
	DWORD nr_bytes;
	BOOL bRet;

	if(conn->size - conn->offset_read <= BUFSIZ)
		nr_bytes = conn->size - conn->offset_read;
	else
		nr_bytes = BUFSIZ;
	
	if(nr_bytes == 0)
		return;
	
	conn->o.Offset = conn->offset_read;
	conn->o.OffsetHigh = 0;
	bRet = ReadFile(
	   conn->fd,
	   conn->send_aux,
	   nr_bytes,
	   &nr_bytes,
	   &conn->o
	);	
}

static enum connection_state send_transmitfile(struct connection *conn, OVERLAPPED *ovp)
{
	DWORD bytes_sent;
	int rc, nr_bytes;
	char abuffer[64];
	BOOL bRet;
	DWORD flags;

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		goto remove_connection;
	}

	bRet = GetOverlappedResult(
			conn->sockfd,
			ovp,
			&bytes_sent,
			FALSE,
			&flags);
	
	if(bytes_sent > conn->send_len - conn->bytes_sent)
		bytes_sent = conn->send_len - conn->bytes_sent;
		
	if (bytes_sent < 0) {		/* error in communication */
		goto remove_connection;
	}

	if (bytes_sent == 0) {		/* connection closed */
		goto remove_connection;
	}
	
	conn->offset += bytes_sent;
	conn->bytes_sent += bytes_sent;
	
	if(conn->bytes_sent == conn->send_len) {
		goto remove_connection;
	}
	
	if(conn->bytes_sent < conn->send_len){
		nr_bytes = conn->size - conn->offset;
		conn->o.Offset = conn->offset;
		conn->o.OffsetHigh = 0;
		bRet = TransmitFile(conn->sockfd, conn->fd, nr_bytes, 0, &conn->o, NULL, 0);
		return STATE_DATA_PARTIAL_SENT;
	}
	
	conn->offset = 0;
	conn->bytes_sent = 0;
	conn->state = STATE_DATA_SENT;
	
	
	return STATE_DATA_SENT;

remove_connection:

	/* remove current connection */
	CloseHandle(conn->fd);
	connection_remove(conn);

	return STATE_CONNECTION_CLOSED;
}

/*
 * Process overlapped I/O operation: data has been received from or
 * has been sent to the socket.
 */

static void handle_aio(struct connection *conn, OVERLAPPED *ovp)
{
	enum connection_state ret_state;
	BOOL bRet;
	int nr_bytes;
	HANDLE hRet;
	
	if (ovp == &conn->send_ov) {
		
		if(conn->caz == 0) {
			ret_state = send_message(conn, ovp);
			if(ret_state == STATE_CONNECTION_CLOSED || ret_state == STATE_DATA_PARTIAL_SENT)
				return;
			CloseHandle(conn->fd);
			connection_remove(conn);
			return;
		} else if(conn->caz == 1 && conn->stare == 0) {
				ret_state = send_message(conn, ovp);
				if(ret_state == STATE_CONNECTION_CLOSED || ret_state == STATE_DATA_PARTIAL_SENT)
					return;
				conn->stare = 1;
				conn->bytes_sent = 0;
				conn->send_len = conn->size;
				
				bRet = TransmitFile(conn->sockfd, conn->fd, conn->size, 0, &conn->o, NULL, 0);
				
		} else if(conn->caz == 1 && conn->stare == 1 ) {	
				ret_state = send_message(conn, ovp);
				if(ret_state == STATE_CONNECTION_CLOSED || ret_state == STATE_DATA_PARTIAL_SENT)
					return;
				
				CloseHandle(conn->fd);
				connection_remove(conn);
				return;
				
		} else if(conn->caz == 2 && conn->stare == 0) {
				ret_state = send_message(conn, ovp);
				if(ret_state == STATE_CONNECTION_CLOSED || ret_state == STATE_DATA_PARTIAL_SENT)
					return;
				conn->stare = 1;
				conn->bytes_sent = 0;
				conn->offset = 0;
				conn->offset_read = 0;
				
				conn->send_aux = malloc(sizeof(char) * BUFSIZ);
				
				hRet = w_iocp_add_key(iocp, conn->fd, (ULONG_PTR) conn);
				send_file_aio(conn);
		} else if(conn->caz == 2 && conn->stare == 1) {
				ret_state = send_message(conn, ovp);
				if(ret_state == STATE_CONNECTION_CLOSED || ret_state == STATE_DATA_PARTIAL_SENT)
					return;

				send_file_aio(conn);
				conn->bytes_sent = 0;
				if(conn->offset_read == conn->size) {
					free(conn->send_aux);
					closesocket(conn->sockfd);
					CloseHandle(conn->fd);
					connection_remove(conn);
				}
		}
				
	} else if (ovp == &conn->recv_ov) {
		
		if(conn->caz == 0 || conn->caz == 1) {
			handle_client_request(conn, ovp);
		}
		
	}else if(ovp == &conn->o) {
		if(conn->caz == 1 && conn->stare == 1 ) {
			ret_state = send_transmitfile( conn, ovp);
			if(ret_state == STATE_CONNECTION_CLOSED || ret_state == STATE_DATA_PARTIAL_SENT)
					return;
		} else if(conn->caz == 2 && conn->stare == 1){
			DWORD nr_read = 0, flags = 0;
		
			bRet = GetOverlappedResult(conn->fd, &conn->o, &nr_read, FALSE, flags);
			conn->offset_read += nr_read;
			memcpy(conn->send_buffer, conn->send_aux, nr_read);
			conn->send_len = nr_read;
			connection_schedule_socket_send(conn, nr_read);
			
		}
	}
		
}

int main(void)
{
	BOOL bRet;
	HANDLE hRet;

	wsa_init();

	/* init multiplexing */
	iocp = w_iocp_create();

	/* create server socket */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT,
		DEFAULT_LISTEN_BACKLOG);

	hRet = w_iocp_add_handle(iocp, (HANDLE) listenfd);

	/* Use AcceptEx to schedule new connection acceptance. */
	create_iocp_accept();
	/* server main loop */
	while (1) {
		OVERLAPPED *ovp;
		ULONG_PTR key;
		DWORD bytes;

		/* wait for events */
		bRet = w_iocp_wait(iocp, &bytes, &key, &ovp);
		if (bRet == FALSE) {
			DWORD err;

			err = GetLastError();
			if (err == ERROR_NETNAME_DELETED) {
				connection_remove((struct connection *) key);
				continue;
			}
		}

		/*
		 * switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */
		if (key == listenfd) {
			handle_new_connection();
		} else {
			handle_aio((struct connection *) key, ovp);
		}
			
	}
	
	wsa_cleanup();
	return 0;
}


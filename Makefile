CFLAGS   = /nologo /W4 /DDEBUG

build: aws.obj sock_util.obj http_parser.obj
	cl $(CFLAGS) /Feaws.exe aws.obj sock_util.obj http_parser.obj wsock32.lib ws2_32.lib

aws.obj: aws.c aws.h
	cl $(CFLAGS) /Foaws.obj /c aws.c 

sock_util.obj: sock_util.c sock_util.h
	cl $(CFLAGS) /Fosock_util.obj /c sock_util.c

http_parser.obj: http_parser.c http_parser.h
	cl $(CFLAGS) /Fohttp_parser.obj /c http_parser.c
	
.PHONY: clean

clean:
	del *.obj aws.exe

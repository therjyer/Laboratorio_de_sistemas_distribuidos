# Makefile para Windows 11 (Utilizando MSYS2 / MinGW-w64)

CC = gcc
CFLAGS = -Wall -Wextra -g -O2
# Para Windows, precisamos da biblioteca ws2_32 para Sockets
LIBS = -lws2_32 
GTK_FLAGS = `pkg-config --cflags --libs gtk+-3.0`

all: server client

server: server_main.c server_core.c
	$(CC) $(CFLAGS) server_main.c server_core.c -o server.exe $(LIBS)

client: client_gui.c
	$(CC) $(CFLAGS) client_gui.c -o client.exe $(GTK_FLAGS) $(LIBS)

clean:
	rm -f server.exe client.exe
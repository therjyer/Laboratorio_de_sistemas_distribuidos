# Compilador e flags
CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
LDFLAGS = -lpthread

# Executáveis a serem gerados
TARGETS = server client

# Regra principal (compila tudo)
all: $(TARGETS)

# Regras de linkagem (geração dos executáveis)
server: server_main.o server_core.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

client: client_gui.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# Regras de compilação (geração dos object objects .o)
server_main.o: server_main.c server_core.h protocol.h
	$(CC) $(CFLAGS) -c $<

server_core.o: server_core.c server_core.h protocol.h
	$(CC) $(CFLAGS) -c $<

client_gui.o: client_gui.c protocol.h
	$(CC) $(CFLAGS) -c $<

# Regra para limpar os ficheiros compilados
clean:
	rm -f *.o $(TARGETS)

# Declaração de regras que não representam ficheiros reais
.PHONY: all clean
#ifndef SERVER_CORE_H
#define SERVER_CORE_H

#include <stdbool.h>

typedef struct Server Server;

Server* Server_Create(int port);

void Server_Destroy(Server* self);

void Server_Start(Server* self);

#endif
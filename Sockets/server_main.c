#include "server_core.h"
#include <stdio.h>
#include <stdlib.h>

#define DEFAULT_PORT 51482

int main(int argc, char *argv[]) {
    int port = DEFAULT_PORT;
    if (argc >= 2) {
        port = atoi(argv[1]);
    }

    Server* my_server = Server_Create(port);
    Server_Start(my_server);
    Server_Destroy(my_server);
    
    return 0;
}
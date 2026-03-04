#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    typedef SOCKET socket_t;
#else
    #include <sys/socket.h>
    #include <unistd.h>
    typedef int socket_t;
#endif

typedef struct {
    uint32_t code_length;
} ClientRequestHeader;

typedef struct {
    uint32_t err_length;
    uint32_t out_length;
} ServerResponseHeader;

static inline int send_all(socket_t sockfd, const void *buf, size_t len) {
    size_t total = 0;
    const char *p = (const char *)buf;
    while (total < len) {
        int n = send(sockfd, p + total, len - total, 0);
        if (n <= 0) return -1;
        total += n;
    }
    return 0;
}

static inline int recv_all(socket_t sockfd, void *buf, size_t len) {
    size_t total = 0;
    char *p = (char *)buf;
    while (total < len) {
        int n = recv(sockfd, p + total, len - total, 0);
        if (n <= 0) return -1;
        total += n;
    }
    return 0;
}

#endif
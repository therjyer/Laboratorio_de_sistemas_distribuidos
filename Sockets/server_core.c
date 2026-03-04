#include "server_core.h"
#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
    #include <winsock2.h>
    #include <windows.h>
    #include <direct.h>
#endif

struct Server {
    int port;
    socket_t listen_fd;
    bool is_running;
};

typedef struct {
    socket_t client_socket;
    struct sockaddr_in client_addr;
} ClientContext;

static char* read_file(const char* filepath, uint32_t* out_len) {
    FILE* f = fopen(filepath, "rb");
    if (!f) {
        *out_len = 0;
        return calloc(1, 1);
    }
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    char* content = malloc(size + 1);
    fread(content, 1, size, f);
    content[size] = '\0';
    fclose(f);
    
    *out_len = (uint32_t)size;
    return content;
}

#ifdef _WIN32
DWORD WINAPI handle_client(LPVOID arg) {
#else
void* handle_client(void* arg) {
#endif
    ClientContext* ctx = (ClientContext*)arg;
    socket_t sock = ctx->client_socket;
    
    #ifdef _WIN32
    printf("[Thread %lu] Cliente conectado: %s\n", GetCurrentThreadId(), inet_ntoa(ctx->client_addr.sin_addr));
    #endif

    ClientRequestHeader req_header;
    if (recv_all(sock, &req_header, sizeof(req_header)) < 0) {
        printf("Falha ao ler cabecalho.\n");
        closesocket(sock); free(ctx); return 0;
    }

    uint32_t code_len = ntohl(req_header.code_length);
    char* code = malloc(code_len + 1);
    
    if (recv_all(sock, code, code_len) < 0) {
        free(code); closesocket(sock); free(ctx); return 0;
    }
    code[code_len] = '\0';

    char temp_path[MAX_PATH];
    GetTempPathA(MAX_PATH, temp_path);
    char work_dir[MAX_PATH];
    sprintf(work_dir, "%sremote_comp_%lu", temp_path, GetCurrentThreadId());
    _mkdir(work_dir);

    char filepath[MAX_PATH];
    sprintf(filepath, "%s\\main.c", work_dir);
    FILE* src_file = fopen(filepath, "w");
    fputs(code, src_file);
    fclose(src_file);
    free(code);

    char cmd[1024];
    sprintf(cmd, "gcc %s\\main.c -o %s\\prog.exe 2> %s\\errors.txt", work_dir, work_dir, work_dir);
    int comp_status = system(cmd);
    
    if (comp_status == 0) {
        sprintf(cmd, "%s\\prog.exe > %s\\output.txt 2>> %s\\errors.txt", work_dir, work_dir, work_dir);
        system(cmd);
    }

    char err_path[MAX_PATH], out_path[MAX_PATH];
    sprintf(err_path, "%s\\errors.txt", work_dir);
    sprintf(out_path, "%s\\output.txt", work_dir);

    uint32_t err_len = 0, out_len = 0;
    char* err_content = read_file(err_path, &err_len);
    char* out_content = read_file(out_path, &out_len);

    ServerResponseHeader res_header;
    res_header.err_length = htonl(err_len);
    res_header.out_length = htonl(out_len);
    send_all(sock, &res_header, sizeof(res_header));

    if (err_len > 0) send_all(sock, err_content, err_len);
    if (out_len > 0) send_all(sock, out_content, out_len);

    free(err_content);
    free(out_content);
    
    sprintf(cmd, "cmd.exe /c rmdir /s /q \"%s\"", work_dir);
    system(cmd);

    closesocket(sock);
    free(ctx);
    #ifdef _WIN32
    printf("[Thread %lu] Conexao encerrada.\n", GetCurrentThreadId());
    #endif
    return 0;
}

Server* Server_Create(int port) {
    #ifdef _WIN32
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2,2), &wsa) != 0) {
        printf("Falha na inicializacao do Winsock.\n");
        exit(1);
    }
    #endif

    Server* self = (Server*)malloc(sizeof(Server));
    self->port = port;
    self->is_running = false;
    
    self->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (self->listen_fd == INVALID_SOCKET) {
        printf("Erro ao criar socket.\n");
        exit(1);
    }

    int opt = 1;
    setsockopt(self->listen_fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(self->port);

    if (bind(self->listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == SOCKET_ERROR) {
        printf("Erro no bind.\n");
        exit(1);
    }

    return self;
}

void Server_Start(Server* self) {
    listen(self->listen_fd, 50);
    self->is_running = true;
    
    printf("Servidor C de Compilacao (Windows) iniciado na porta %d...\n", self->port);

    while (self->is_running) {
        struct sockaddr_in cli_addr;
        int clilen = sizeof(cli_addr);
        socket_t newsockfd = accept(self->listen_fd, (struct sockaddr*)&cli_addr, &clilen);
        
        if (newsockfd == INVALID_SOCKET) continue;

        ClientContext* ctx = malloc(sizeof(ClientContext));
        ctx->client_socket = newsockfd;
        ctx->client_addr = cli_addr;

        #ifdef _WIN32
        HANDLE thread = CreateThread(NULL, 0, handle_client, ctx, 0, NULL);
        if (thread) CloseHandle(thread);
        #endif
    }
}

void Server_Destroy(Server* self) {
    if (self) {
        closesocket(self->listen_fd);
        free(self);
        #ifdef _WIN32
        WSACleanup();
        #endif
    }
}
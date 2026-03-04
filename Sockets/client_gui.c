#include <gtk/gtk.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "protocol.h"

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
#endif

GtkWidget *text_view_code;
GtkWidget *text_view_errors;
GtkWidget *text_view_output;

const char* SERVER_IP = "10.16.0.71";
const int SERVER_PORT = 51482;

static void on_run_clicked(GtkWidget *widget, gpointer data) {
    (void)widget; (void)data;

    GtkTextBuffer *buffer_code = gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view_code));
    GtkTextIter start, end;
    gtk_text_buffer_get_bounds(buffer_code, &start, &end);
    char *code_text = gtk_text_buffer_get_text(buffer_code, &start, &end, FALSE);

    #ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2,2), &wsa);
    #endif

    socket_t sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == (socket_t)-1) {
        gtk_text_buffer_set_text(gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view_errors)), "Falha ao criar socket.", -1);
        g_free(code_text); return;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT);
    serv_addr.sin_addr.s_addr = inet_addr(SERVER_IP);

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        gtk_text_buffer_set_text(gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view_errors)), "Falha ao conectar. O servidor esta a correr?", -1);
        #ifdef _WIN32
        closesocket(sockfd); WSACleanup();
        #endif
        g_free(code_text); return;
    }

    gtk_text_buffer_set_text(gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view_errors)), "Compilando...", -1);
    gtk_text_buffer_set_text(gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view_output)), "", -1);

    size_t code_len = strlen(code_text);
    ClientRequestHeader req;
    req.code_length = htonl((uint32_t)code_len);
    
    send_all(sockfd, &req, sizeof(req));
    send_all(sockfd, code_text, code_len);
    g_free(code_text);

    ServerResponseHeader res;
    if (recv_all(sockfd, &res, sizeof(res)) < 0) {
        gtk_text_buffer_set_text(gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view_errors)), "Erro de comunicacao com o servidor.", -1);
        #ifdef _WIN32
        closesocket(sockfd); WSACleanup();
        #endif
        return;
    }

    uint32_t err_len = ntohl(res.err_length);
    uint32_t out_len = ntohl(res.out_length);

    char *err_str = malloc(err_len + 1);
    char *out_str = malloc(out_len + 1);
    
    if (err_len > 0) recv_all(sockfd, err_str, err_len);
    if (out_len > 0) recv_all(sockfd, out_str, out_len);
    
    err_str[err_len] = '\0';
    out_str[out_len] = '\0';

    gtk_text_buffer_set_text(gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view_errors)), err_str, -1);
    gtk_text_buffer_set_text(gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view_output)), out_str, -1);

    free(err_str); free(out_str);
    
    #ifdef _WIN32
    closesocket(sockfd);
    WSACleanup();
    #endif
}

GtkWidget* create_labeled_textview(const char* title, GtkWidget** textview_ptr) {
    GtkWidget *box = gtk_box_new(GTK_ORIENTATION_VERTICAL, 2);
    GtkWidget *label = gtk_label_new(title);
    gtk_label_set_xalign(GTK_LABEL(label), 0.0);
    gtk_box_pack_start(GTK_BOX(box), label, FALSE, FALSE, 0);

    GtkWidget *scroll = gtk_scrolled_window_new(NULL, NULL);
    *textview_ptr = gtk_text_view_new();
    gtk_container_add(GTK_CONTAINER(scroll), *textview_ptr);
    gtk_box_pack_start(GTK_BOX(box), scroll, TRUE, TRUE, 0);

    return box;
}

static void activate(GtkApplication *app, gpointer user_data) {
    (void)user_data;
    GtkWidget *window = gtk_application_window_new(app);
    gtk_window_set_title(GTK_WINDOW(window), "C IDE Remota - Cliente Windows");
    gtk_window_set_default_size(GTK_WINDOW(window), 800, 600);
    gtk_container_set_border_width(GTK_CONTAINER(window), 10);

    GtkWidget *vbox = gtk_box_new(GTK_ORIENTATION_VERTICAL, 5);
    gtk_container_add(GTK_CONTAINER(window), vbox);

    GtkWidget *code_box = create_labeled_textview("Area de Edicao de Codigo (C):", &text_view_code);
    gtk_box_pack_start(GTK_BOX(vbox), code_box, TRUE, TRUE, 0);

    GtkTextBuffer *buffer = gtk_text_view_get_buffer(GTK_TEXT_VIEW(text_view_code));
    gtk_text_buffer_set_text(buffer, "#include <stdio.h>\n\nint main() {\n    printf(\"Ola do Windows 11!\\n\");\n    return 0;\n}", -1);

    GtkWidget *button_box = gtk_button_box_new(GTK_ORIENTATION_HORIZONTAL);
    gtk_button_box_set_layout(GTK_BUTTON_BOX(button_box), GTK_BUTTONBOX_START);
    GtkWidget *btn_run = gtk_button_new_with_label("Compilar e Executar no Servidor");
    g_signal_connect(btn_run, "clicked", G_CALLBACK(on_run_clicked), NULL);
    gtk_container_add(GTK_CONTAINER(button_box), btn_run);
    gtk_box_pack_start(GTK_BOX(vbox), button_box, FALSE, FALSE, 5);

    GtkWidget *hbox_bottom = gtk_box_new(GTK_ORIENTATION_HORIZONTAL, 5);
    gtk_box_pack_start(GTK_BOX(vbox), hbox_bottom, TRUE, TRUE, 0);

    GtkWidget *err_box = create_labeled_textview("Erros de Compilacao:", &text_view_errors);
    gtk_box_pack_start(GTK_BOX(hbox_bottom), err_box, TRUE, TRUE, 0);

    GtkWidget *out_box = create_labeled_textview("Saida da Execucao (Stdout):", &text_view_output);
    gtk_box_pack_start(GTK_BOX(hbox_bottom), out_box, TRUE, TRUE, 0);

    gtk_widget_show_all(window);
}

int main(int argc, char **argv) {
    GtkApplication *app = gtk_application_new("com.exemplo.socketsc", G_APPLICATION_FLAGS_NONE);
    g_signal_connect(app, "activate", G_CALLBACK(activate), NULL);
    int status = g_application_run(G_APPLICATION(app), argc, argv);
    g_object_unref(app);
    return status;
}

#include "gfserver-student.h"
#include <stdlib.h>
#define BUFSIZE 4096
// Modify this file to implement the interface specified in
 // gfserver.h.
struct gfserver_t
{
    void *handlerarg;
    gfh_error_t (*handler)(gfcontext_t **, const char *, void *);
    int max_npending;
    unsigned short port;
};

struct gfcontext_t
{
    int client_socket, server_socket;
    char *filepath;
};

void gfs_abort(gfcontext_t **ctx){
    close((*ctx)->client_socket);
}

gfserver_t* gfserver_create(){
    // dummy for now - need to fill this part in
    gfserver_t *gfs = (gfserver_t *)malloc(sizeof(gfserver_t));
    return gfs;
}

ssize_t gfs_send(gfcontext_t **ctx, const void *data, size_t len){
    // not yet implemented
    ssize_t sendMsg, counter = 0;
    while (counter < len) {
        sendMsg = send((*ctx)->client_socket, data + counter, len - counter, 0);
        if (sendMsg < 0) {
            perror("server send failed\n");
        }
        counter += sendMsg;

    }
    return counter;
}

ssize_t gfs_sendheader(gfcontext_t **ctx, gfstatus_t status, size_t file_len){
    // not yet implemented
    ssize_t sendMsg;
    char *header = (char *)malloc(strlen("GETFILE GET ") + 38); //15 for FNF, 20 for max integer length, 3 for spaces
    const char *strstatus = NULL;

    switch (status) {
        default: {
        strstatus = "UNKNOWN";
        } break;

        case GF_FILE_NOT_FOUND: {
        strstatus = "FILE_NOT_FOUND";
        } break;

        case GF_INVALID: {
        strstatus = "INVALID";
        } break;

        case GF_ERROR: {
        strstatus = "ERROR";
        } break;

        case GF_OK: {
        strstatus = "OK";
        } break;
    }
    if (status == GF_OK) {
        sprintf(header, "GETFILE %s %zu\r\n\r\n", strstatus, file_len);
    }
    else {
        sprintf(header, "GETFILE %s\r\n\r\n", strstatus);
    }
    printf("send header %s",header);
    sendMsg = send((*ctx)->client_socket, (void *)header, strlen(header), 0);
    if (sendMsg < 0) {
        perror("Failed to send header\n");
        gfs_abort(ctx);
    }
    free(header);
    return sendMsg;
}

void gfserver_serve(gfserver_t **gfs){
    int server_socket, client_socket;
    int opt = 1;
    char buffer[BUFSIZE];
    char *buffer_pointer;
    struct addrinfo hints, *servinfo, *p;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    int rv;
    int header_received = 0;
    char portnum[6];
    char temp_path[255];
    char temp_path_plus_one[256];
    int rcvMsg, scn_return;

    sprintf(portnum, "%d", (*gfs)->port);
    rv = getaddrinfo(NULL, portnum, &hints, &servinfo);
    if ( rv != 0) {
        printf("getaddrinfo: %s\n", gai_strerror(rv));
        return;
    }

    for (p = servinfo; p!= NULL; p = p->ai_next) {
        server_socket = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if ( server_socket == -1) {
            perror("server: socket\n");
            continue;
        }

        if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt) == -1) {
            perror("setsockopt\n");
            break;
        }

        if (bind(server_socket, p->ai_addr, p->ai_addrlen) == -1) {
            perror("bind failed\n");
            continue;
        }
        break;
    }

    freeaddrinfo(servinfo);

    if (listen(server_socket, 3) < 0) {
        printf("listen\n");
    }
    gfcontext_t *ctx = (gfcontext_t *)malloc(sizeof(gfcontext_t));
    buffer_pointer = buffer;
    
    while (1) {
        client_socket = accept(server_socket, NULL, NULL);
        if (client_socket < 0) {
            printf("accept\n");
            free(ctx);
            break;
        }
        else {
            ctx->client_socket = client_socket;
            ctx->server_socket = server_socket;
        }
        header_received = 0;
        while (header_received == 0){
            rcvMsg = recv(ctx->client_socket, buffer_pointer, BUFSIZE, 0);
            if (rcvMsg < 0) {
                printf("failed to receive header\n");
                gfs_abort(&ctx);
                free(ctx);
                break;
            }
            else if (rcvMsg == 0) {
                printf("header received 0\n");
                gfs_abort(&ctx);
                free(ctx);
                break;
            }
            
            scn_return = sscanf(buffer, "GETFILE GET /%s\r\n\r\n", temp_path);
            printf("%s\n",buffer);
            if (strstr(buffer, "\r\n\r\n") != NULL){
                printf("header received\n");
                header_received = 1;
                if (scn_return != 1) {
                    printf("malformed header\n");
                    gfs_sendheader(&ctx, GF_INVALID, 0);
                    gfs_abort(&ctx);
                    free(ctx);
                } 
                else {
                    snprintf(temp_path_plus_one, sizeof(temp_path_plus_one), "/%s", temp_path);
                    ctx->filepath = temp_path_plus_one;
                    (*gfs)->handler(&ctx,ctx->filepath, (*gfs)->handlerarg);
                }
            }
            else {
                printf("received partial header\n");
                buffer_pointer += rcvMsg;
            }
        }
    }
    free(ctx);
    free(*gfs);
}

void gfserver_set_handlerarg(gfserver_t **gfs, void* arg){
    (*gfs)->handlerarg = arg;
}

void gfserver_set_handler(gfserver_t **gfs, gfh_error_t (*handler)(gfcontext_t **, const char *, void*)){
    (*gfs)->handler = handler;
}

void gfserver_set_maxpending(gfserver_t **gfs, int max_npending){
    (*gfs)->max_npending = max_npending;
}

void gfserver_set_port(gfserver_t **gfs, unsigned short port){
    (*gfs)->port = port;
}



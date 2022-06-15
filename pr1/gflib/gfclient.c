
#include <stdlib.h>

#include "gfclient-student.h"
#define BUFSIZE 4096
 // Modify this file to implement the interface specified in
 // gfclient.h.
struct gfcrequest_t {
  void *headerarg;
  void (*headerfunc)(void *, size_t, void *);
  const char *path;
  unsigned short port;
  const char *server;
  void *writearg;
  void (*writefunc)(void *, size_t, void *);
  gfstatus_t status;
  size_t bytesreceived;
  size_t filelen;
};
// optional function for cleaup processing.
void gfc_cleanup(gfcrequest_t **gfr) {
  free((*gfr));
  (*gfr) = NULL;
}

gfcrequest_t *gfc_create() {
  // dummy for now - need to fill this part in
  gfcrequest_t *gfr = (gfcrequest_t *)malloc(sizeof(gfcrequest_t));
  gfr->headerarg = NULL;
  gfr->headerfunc = NULL;
  gfr->writearg = NULL;
  gfr->writefunc = NULL;
  return gfr;
}

size_t gfc_get_bytesreceived(gfcrequest_t **gfr) {
  // not yet implemented
  return (*gfr)->bytesreceived;
}

size_t gfc_get_filelen(gfcrequest_t **gfr) {
  // not yet implemented
  return (*gfr)->filelen;
}

gfstatus_t gfc_get_status(gfcrequest_t **gfr) {
  // not yet implemented
  return (*gfr)->status;
}

void gfc_global_init() {}

void gfc_global_cleanup() {}

int gfc_perform(gfcrequest_t **gfr) {
  /* currently not implemented.  You fill this part in. */
  int sock;
  char buffer[BUFSIZE] = {0};
  struct addrinfo hints, *servinfo, *p;
  int rv, rcvMsg, sendMsg,scan_return;
  int header_read = 0;
  char portnum[6];
  char header[256];
  char *filedata;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  size_t filelen = 0, headerlen = 0;
  char request_status[15];
  char *request_str = (char *)malloc(strlen("GETFILE GET") + strlen((*gfr)->path) + strlen("\r\n\r\n") + 3);
  sprintf(request_str,"GETFILE GET %s\r\n\r\n", (*gfr)->path);

  setbuf(stdout, NULL);  // disable buffering

  if (((*gfr)->port < 1025) || ((*gfr)->port > 65535)) {
    fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__,
            (*gfr)->port);
    exit(1);
  }
  sprintf(portnum, "%d", (*gfr)->port);
  if ((rv = getaddrinfo(NULL, portnum, &hints, &servinfo)) != 0) {
    printf("getaddrinnfo\n");
    freeaddrinfo(servinfo);
    free(request_str);
    return 1;
  }

  for (p = servinfo; p!= NULL; p = p->ai_next) {
    sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (sock == -1) {
      printf("client: socket\n");
      continue;
    }

    if (connect(sock, p->ai_addr, p->ai_addrlen) == -1) {
      close(sock);
      perror("client connect\n");
      freeaddrinfo(servinfo);
      free(request_str);
      return -1;
    }
    break;
  }

  if(p == NULL) {
    printf("client failed to connect\n");
    freeaddrinfo(servinfo);
    free(request_str);
    return 2;
  }
  freeaddrinfo(servinfo);
  sendMsg = send(sock, request_str, strlen(request_str), 0);
  if (sendMsg < 0) {
    printf("client send error\n");
    return -1;
  }
  free(request_str);
  int bytes_received = 0;
  while(1) {
    rcvMsg = recv(sock, buffer, BUFSIZE, 0);
    if (rcvMsg == -1) {
      printf("client failed to receive a message\n");
      exit(1);
    }
    if (header_read == 0 ){
      scan_return = sscanf(buffer, "GETFILE %s %zu\r\n\r\n", request_status, &filelen);
      if (scan_return < 2 || scan_return == EOF) {
        scan_return = sscanf(buffer, "GETFILE %s\r\n\r\n", request_status);
        if (strncmp(request_status,"OK",strlen("OK")) == 0) {
          (*gfr)->status = GF_OK;
        }
        else if (strncmp(request_status,"FILE_NOT_FOUND",strlen("FILE_NOT_FOUND")) == 0){
          (*gfr)->status = GF_FILE_NOT_FOUND;
          close(sock);
          return 0;
        }
        else if (strncmp(request_status,"ERROR",strlen("ERROR")) == 0){
          (*gfr)->status = GF_ERROR;
          close(sock);
          return 0;
        }
        else {
          (*gfr)->status = GF_INVALID;
          close(sock);
          return -1;
        }
        printf("status error");
        return -1;
      }
      else {
        //header = (char *)malloc(strlen("GETFILE ") + strlen(request_status))
        sprintf(header,"GETFILE %s %zu\r\n\r\n", request_status, filelen);
        headerlen = strlen(header);
        if ((*gfr)->headerarg != NULL) {
          (*gfr)->headerfunc(header,headerlen,(*gfr)->headerarg);
        }
        if (strncmp(request_status,"OK",strlen("OK")) == 0) {
          (*gfr)->status = GF_OK;
        }
        else if (strncmp(request_status,"FILE_NOT_FOUND",strlen("FILE_NOT_FOUND")) == 0){
          (*gfr)->status = GF_FILE_NOT_FOUND;
          close(sock);
          return 0;
        }
        else if (strncmp(request_status,"ERROR",strlen("ERROR")) == 0){
          (*gfr)->status = GF_ERROR;
          close(sock);
          return 0;
        }
        else {
          (*gfr)->status = GF_INVALID;
          close(sock);
          return -1;
        }
        (*gfr)->filelen = filelen;
        bytes_received += rcvMsg - headerlen;
        (*gfr)->bytesreceived = bytes_received;
        filedata = buffer + headerlen;
        (*gfr)->writefunc((void *)filedata, (*gfr)->bytesreceived, (*gfr)->writearg);
        if ((*gfr)->bytesreceived >= (*gfr)->filelen) {
          printf("file transfer complete\n");
          close(sock);
          return 0;
        }
        memset(&buffer, 0, sizeof buffer);
      }
      header_read = 1;
    }
    else {
      if (rcvMsg == 0) {
        close(sock);
        printf("no msg received\n");
        return -1;
      }
      bytes_received += rcvMsg;
      (*gfr)->bytesreceived = bytes_received;
      (*gfr)->writefunc((void *)buffer, rcvMsg, (*gfr)->writearg);
      if ((*gfr)->bytesreceived >= (*gfr)->filelen) {
        printf("file transfer complete\n");
        close(sock);
        return 0;
      }
      memset(&buffer, 0, sizeof buffer);
    }
  }
  return 0;
}

void gfc_set_headerarg(gfcrequest_t **gfr, void *headerarg) {
  (*gfr)->headerarg = headerarg;
}

void gfc_set_headerfunc(gfcrequest_t **gfr,
                        void (*headerfunc)(void *, size_t, void *)) {
  (*gfr)->headerfunc = headerfunc;
}

void gfc_set_path(gfcrequest_t **gfr, const char *path) {
  (*gfr)->path = path;
}

void gfc_set_port(gfcrequest_t **gfr, unsigned short port) {
  (*gfr)->port = port;
}

void gfc_set_server(gfcrequest_t **gfr, const char *server) {
  (*gfr)->server = server;
}

void gfc_set_writearg(gfcrequest_t **gfr, void *writearg) {
  (*gfr)->writearg = writearg;
}

void gfc_set_writefunc(gfcrequest_t **gfr,
                       void (*writefunc)(void *, size_t, void *)) {
  (*gfr)->writefunc = writefunc;
}

const char *gfc_strstatus(gfstatus_t status) {
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

  return strstatus;
}
#include <errno.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <getopt.h>
#include <netdb.h>

#define BUFSIZE 16

#define USAGE                                                        \
  "usage:\n"                                                         \
  "  echoserver [options]\n"                                         \
  "options:\n"                                                       \
  "  -p                  Port (Default: 10823)\n"                    \
  "  -m                  Maximum pending connections (default: 5)\n" \
  "  -h                  Show this help message\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"help", no_argument, NULL, 'h'},
    {"port", required_argument, NULL, 'p'},
    {"maxnpending", required_argument, NULL, 'm'},
    {NULL, 0, NULL, 0}};

int main(int argc, char **argv) {
  int maxnpending = 5;
  int option_char;
  int portno = 10823; /* port to listen on */

  // Parse and set command line arguments
  while ((option_char =
              getopt_long(argc, argv, "hx:m:p:", gLongOptions, NULL)) != -1) {
    switch (option_char) {
      case 'm':  // server
        maxnpending = atoi(optarg);
        break;
      case 'p':  // listen-port
        portno = atoi(optarg);
        break;
      case 'h':  // help
        fprintf(stdout, "%s ", USAGE);
        exit(0);
        break;
      default:
        fprintf(stderr, "%s ", USAGE);
        exit(1);
    }
  }

  setbuf(stdout, NULL);  // disable buffering

  if ((portno < 1025) || (portno > 65535)) {
    fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__,
            portno);
    exit(1);
  }
  if (maxnpending < 1) {
    fprintf(stderr, "%s @ %d: invalid pending count (%d)\n", __FILE__, __LINE__,
            maxnpending);
    exit(1);
  }

  int server_fd, newsocket;
  int opt = 1;
  char buffer[BUFSIZE] = {0};
  
  struct addrinfo hints, *servinfo, *p;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  int rv, numbytes;
  char portnum[6];
  int sendMsg;

  sprintf(portnum, "%d", portno);
  rv = getaddrinfo(NULL, portnum, &hints, &servinfo);
  if ( rv != 0) {
    printf("getaddrinfo: %s\n", gai_strerror(rv));
  }

  for (p = servinfo; p!= NULL; p = p->ai_next) {
    server_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if ( server_fd == -1) {
      perror("server: socket");
      continue;
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt) == -1) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    if (bind(server_fd, p->ai_addr, p->ai_addrlen) == -1) {
        perror("bind failed");
        continue;
    }
    break;
  }

  freeaddrinfo(servinfo);

  if(p==NULL) {
    printf("server failed to bind\n");
  }

  if (listen(server_fd, maxnpending) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }

  while(1) {
    if ((newsocket = accept(server_fd, NULL, NULL)) < 0) {
      perror("accept");
      continue;
    }
    if ((numbytes = recv(newsocket, buffer, BUFSIZE-1, 0)) == -1)  {
      perror("recv");
      exit(EXIT_FAILURE);
    }

    buffer[numbytes] = '\0';

    //printf("%s",buffer );

    sendMsg = send(newsocket, buffer, BUFSIZE-1, 0);
    if (sendMsg != BUFSIZE-1)
    {
        fprintf(stderr, "Server socket failed to send echo message. Echo message size: %d", sendMsg);
        exit(1);
    }

    // printf("SERVER SUCCESSFULLY RECEIVED AND ECHOED MESSAGE BACK TO CLIENT!\n");
    // printf("Message echoed: %s\n", msg_buffer);

    close(newsocket);
  }

  close(server_fd);
  
  return 0;
}


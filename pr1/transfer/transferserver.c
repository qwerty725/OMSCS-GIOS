#include <sys/socket.h>
#include <sys/types.h>
#include <errno.h>
#include <getopt.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BUFSIZE 512

#define USAGE                                            \
  "usage:\n"                                             \
  "  transferserver [options]\n"                         \
  "options:\n"                                           \
  "  -h                  Show this help message\n"       \
  "  -f                  Filename (Default: 6200.txt)\n" \
  "  -p                  Port (Default: 10823)\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"help", no_argument, NULL, 'h'},
    {"port", required_argument, NULL, 'p'},
    {"filename", required_argument, NULL, 'f'},
    {NULL, 0, NULL, 0}};

int main(int argc, char **argv) {
  char *filename = "6200.txt"; // file to transfer 
  int portno = 10823;          // port to listen on 
  int option_char;

  setbuf(stdout, NULL);  // disable buffering

  // Parse and set command line arguments
  while ((option_char =
              getopt_long(argc, argv, "hf:xp:", gLongOptions, NULL)) != -1) {
    switch (option_char) {
      case 'p':  // listen-port
        portno = atoi(optarg);
        break;
      case 'h':  // help
        fprintf(stdout, "%s", USAGE);
        exit(0);
        break;
      default:
        fprintf(stderr, "%s", USAGE);
        exit(1);
      case 'f':  // file to transfer
        filename = optarg;
        break;
    }
  }

  if ((portno < 1025) || (portno > 65535)) {
    fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__,
            portno);
    exit(1);
  }

  if (NULL == filename) {
    fprintf(stderr, "%s @ %d: invalid filename\n", __FILE__, __LINE__);
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
  int rv;
  char portnum[6];
  int sendMsg;
  size_t filelen, counter;

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

  if (listen(server_fd, 3) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }

  while(1) {
    if ((newsocket = accept(server_fd, NULL, NULL)) < 0) {
      perror("accept");
      continue;
    }
    FILE *serverfile;
    serverfile = fopen(filename, "r");
    while(!feof(serverfile)) {
      filelen = fread(buffer, sizeof(char), BUFSIZE-1, serverfile);
      if (filelen < 0) {
        printf("server read fail");
        exit(EXIT_FAILURE);
      }
      counter = 0;
      while(counter < filelen) {
        // keep track of what was sent
        sendMsg = send(newsocket, buffer + counter, filelen - counter, 0);
        if (sendMsg < 0) {
          printf("server failure to send");
          exit(EXIT_FAILURE);
        }

        counter += sendMsg;

      }
      memset(buffer, 0, BUFSIZE);
    }
    close(newsocket);
    fclose(serverfile);
  }
  close(server_fd);
  return 0;
}

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <getopt.h>
#include <netdb.h>
#include <netinet/in.h>
#include <unistd.h>

#define BUFSIZE 512

#define USAGE                                                \
  "usage:\n"                                                 \
  "  transferclient [options]\n"                             \
  "options:\n"                                               \
  "  -h                  Show this help message\n"           \
  "  -p                  Port (Default: 10823)\n"            \
  "  -s                  Server (Default: localhost)\n"      \
  "  -o                  Output file (Default cs6200.txt)\n" 

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {{"server", required_argument, NULL, 's'},
                                       {"output", required_argument, NULL, 'o'},
                                       {"port", required_argument, NULL, 'p'},
                                       {"help", no_argument, NULL, 'h'},
                                       {NULL, 0, NULL, 0}};

/* Main ========================================================= */
int main(int argc, char **argv) {
  unsigned short portno = 10823;
  int option_char = 0;
  char *hostname = "localhost";
  char *filename = "cs6200.txt";

  setbuf(stdout, NULL);

  /* Parse and set command line arguments */ 
  while ((option_char =
              getopt_long(argc, argv, "xp:s:h:o:", gLongOptions, NULL)) != -1) {
    switch (option_char) {
      case 's':  // server
        hostname = optarg;
        break;
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
      case 'o':  // filename
        filename = optarg;
        break;
    }
  }

  if (NULL == hostname) {
    fprintf(stderr, "%s @ %d: invalid host name\n", __FILE__, __LINE__);
    exit(1);
  }

  if (NULL == filename) {
    fprintf(stderr, "%s @ %d: invalid filename\n", __FILE__, __LINE__);
    exit(1);
  }

  if ((portno < 1025) || (portno > 65535)) {
    fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__,
            portno);
    exit(1);
  }

  int sock;
  char buffer[BUFSIZE] = {0};
  struct addrinfo hints, *servinfo, *p;
  int rv, rcvMsg;
  char portnum[6];
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  int writelen;

  setbuf(stdout, NULL);  // disable buffering

  if ((portno < 1025) || (portno > 65535)) {
    fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__,
            portno);
    exit(1);
  }

  if (NULL == hostname) {
    fprintf(stderr, "%s @ %d: invalid host name\n", __FILE__, __LINE__);
    exit(1);
  }
  sprintf(portnum, "%d", portno);
  if ((rv = getaddrinfo(NULL, portnum, &hints, &servinfo)) != 0) {
    printf("getaddrinnfo");
    return 1;
  }


  for (p = servinfo; p!= NULL; p = p->ai_next) {
    sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (sock == -1) {
      perror("client: socket");
      continue;
    }

    if (connect(sock, p->ai_addr, p->ai_addrlen) == -1) {
      close(sock);
      perror("client connect");
      continue;
    }
    break;
  }

  if(p == NULL) {
    printf("client failed to connect\n");
    return 2;
  }
  freeaddrinfo(servinfo);

  //remove(filename);
  FILE *clientfile;
  clientfile = fopen(filename,"w+");

  rcvMsg = 1;

  while (rcvMsg > 0) {
    rcvMsg = recv(sock, buffer, BUFSIZE - 1, 0);
    if (rcvMsg == -1) {
      printf("client fialed to receive a message\n");
      exit(1);
    }
    writelen = fwrite(buffer, 1, rcvMsg, clientfile);

    if (writelen != rcvMsg) {
      printf("write length doest not equat received message\n Written %i Message %i", writelen, rcvMsg);
      exit(1);
    }
  }

  fclose(clientfile);
  close(sock);
  printf("successful write");
  return 0;
}

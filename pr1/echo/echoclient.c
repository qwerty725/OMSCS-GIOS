#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <getopt.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>

// A buffer large enough to contain the longest allowed string 
#define BUFSIZE 16

#define USAGE                                                          \
  "usage:\n"                                                           \
  "  echoclient [options]\n"                                           \
  "options:\n"                                                         \
  "  -s                  Server (Default: localhost)\n"                \
  "  -p                  Port (Default: 10823)\n"                      \
  "  -m                  Message to send to server (Default: \"Hello " \
  "Summer.\")\n"                                                       \
  "  -h                  Show this help message\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"server", required_argument, NULL, 's'},
    {"port", required_argument, NULL, 'p'},
    {"message", required_argument, NULL, 'm'},
    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}};

/* Main ========================================================= */
int main(int argc, char **argv) {
  unsigned short portno = 10823;
  int option_char = 0;
  char *message = "Hello Summer!!";
  char *hostname = "localhost";

  // Parse and set command line arguments
  while ((option_char =
              getopt_long(argc, argv, "p:s:m:hx", gLongOptions, NULL)) != -1) {
    switch (option_char) {
      default:
        fprintf(stderr, "%s", USAGE);
        exit(1);
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
      case 'm':  // message
        message = optarg;
        break;
    }
  }
  int sock;
  char buffer[BUFSIZE] = {0};
  struct addrinfo hints, *servinfo, *p;
  int rv, sendMsg, rcvMsg;
  char portnum[6];
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  setbuf(stdout, NULL);  // disable buffering

  if ((portno < 1025) || (portno > 65535)) {
    fprintf(stderr, "%s @ %d: invalid port number (%d)\n", __FILE__, __LINE__,
            portno);
    exit(1);
  }

  if (NULL == message) {
    fprintf(stderr, "%s @ %d: invalid message\n", __FILE__, __LINE__);
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

  sendMsg = send(sock, message, BUFSIZE-2, 0);

  if (sendMsg != BUFSIZE-2){
    fprintf(stderr, "Failure to send() correct number of bytes\n");
    exit(1);
  }
        
  rcvMsg = recv(sock, buffer, BUFSIZE-1, 0);
  if (rcvMsg == -1)
  {
      fprintf(stderr, "Client socket failed to receive echo message\n");
      exit(1);
  }

  buffer[rcvMsg] = '\0'; // null terminate message in buffer

  // printf("Client socket successfully received echo from server socket!\n");

  printf("%s",buffer); //client echo

  close(sock);

  return 0;


}

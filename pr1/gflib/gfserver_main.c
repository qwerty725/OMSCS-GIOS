#include <unistd.h>
#include <errno.h>
#include <getopt.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <sys/types.h>
#include "content.h"
#include "gfserver-student.h"
#include "gfserver.h"

#define USAGE                                                          \
  "usage:\n"                                                           \
  "  gfserver_main [options]\n"                                        \
  "options:\n"                                                         \
  "  -h                  Show this help message.\n"                    \
  "  -m [content_file]   Content file mapping keys to content files\n" \
  "  -p [listen_port]    Listen port (Default: 10823)\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"help", no_argument, NULL, 'h'},
    {"port", required_argument, NULL, 'p'},
    {"content", required_argument, NULL, 'm'},
    {NULL, 0, NULL, 0}};

/* Main ========================================================= */
int main(int argc, char **argv) {
  char *content_map_file = "content.txt";
  gfserver_t *gfs;
  int option_char = 0;
  unsigned short port = 10823;

  setbuf(stdout, NULL);  // disable caching

  // Parse and set command line arguments
  while ((option_char =
              getopt_long(argc, argv, "hrt:p:m:", gLongOptions, NULL)) != -1) {
    switch (option_char) {
      default:
        fprintf(stderr, "%s", USAGE);
        exit(1);
      case 'p':  /* listen-port */
        port = atoi(optarg);
        break;
      case 'h':  /* help */
        fprintf(stdout, "%s", USAGE);
        exit(0);
        break;
      case 'm':  /* file-path */
        content_map_file = optarg;
        break;
    }
  }

  content_init(content_map_file);

  /*Initializing server*/
  gfs = gfserver_create();

  /*Setting options*/
  gfserver_set_handler(&gfs, gfs_handler);
  gfserver_set_port(&gfs, port);
  gfserver_set_maxpending(&gfs, 86);

  /* this implementation does not pass any extra state, so it uses NULL. */
  /* this value could be non-NULL.  You might want to test that in your own
   * code. */
  gfserver_set_handlerarg(&gfs, NULL);

  // Loops forever
  gfserver_serve(&gfs);
}


#include <stdlib.h>

#include "gfserver-student.h"
#define BUFSIZE 4096   
#define USAGE                                                                                \
  "usage:\n"                                                                                 \
  "  gfserver_main [options]\n"                                                              \
  "options:\n"                                                                               \
  "  -t [nthreads]       Number of threads (Default: 7)\n"                                   \
  "  -p [listen_port]    Listen port (Default: 20801)\n"                                     \
  "  -m [content_file]   Content file mapping keys to content files\n"                       \
  "  -d [delay]          Delay in content_get, default 0, range 0-5000000 (microseconds)\n " \
  "  -h                  Show this help message.\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"help", no_argument, NULL, 'h'},
    {"content", required_argument, NULL, 'm'},
    {"nthreads", required_argument, NULL, 't'},
    {"port", required_argument, NULL, 'p'},
    {"delay", required_argument, NULL, 'd'},
    {NULL, 0, NULL, 0}};

extern unsigned long int content_delay;

extern gfh_error_t gfs_handler(gfcontext_t **ctx, const char *path, void *arg);

pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_populated_cond = PTHREAD_COND_INITIALIZER;
steque_t *request_queue;

static void _sig_handler(int signo) {
  if ((SIGINT == signo) || (SIGTERM == signo)) {
    exit(signo);
  }
}

void *thread_func(void *arg) {
  request_t *request;
  int filedesc, sendMsg;
  struct stat file_stat;
  long int file_size, file_bytes_read = 0, file_bytes_sent = 0;
  char buffer[BUFSIZE];
  while(1) {
    file_bytes_read = 0;
    file_bytes_sent = 0;
    //lock mutex and wait for queue populate
    pthread_mutex_lock(&queue_mutex);
    while (steque_isempty(request_queue)) {
      pthread_cond_wait(&queue_populated_cond, &queue_mutex);
    }

    request = steque_pop(request_queue);
    pthread_mutex_unlock(&queue_mutex);

    //printf("thread %p starting\n", arg);
    filedesc = content_get(request->path);
    if (filedesc < 0){
      //send FNF
      printf("server thread file not found\n");
      gfs_sendheader(&request->ctx, GF_FILE_NOT_FOUND, 0);
      continue;
    }
    //file length
    fstat(filedesc, &file_stat);
    file_size = file_stat.st_size;

    //send header
    gfs_sendheader(&request->ctx, GF_OK, file_size);
    while (file_bytes_sent < file_size) {
      file_bytes_read = pread(filedesc, buffer, BUFSIZE, file_bytes_sent);
      if(file_bytes_read < 0) {
        printf("file read error\n");
        break;
      }
      sendMsg = gfs_send(&request->ctx, buffer, file_bytes_read);
      if (sendMsg < 0) {
        printf("Server failed to send\n");
        free(request);
        break;
      }
      file_bytes_sent += file_bytes_read;
      memset(&buffer, 0, sizeof(buffer));
    }
    free(request->ctx);
    //free(&request->path);
    free(request);

  }
  
}
/* Main ========================================================= */
int main(int argc, char **argv) {
  gfserver_t *gfs = NULL;
  int nthreads = 12;
  int option_char = 0;
  unsigned short port = 10823;
  char *content_map = "content.txt";

  setbuf(stdout, NULL);

  if (SIG_ERR == signal(SIGINT, _sig_handler)) {
    fprintf(stderr, "Can't catch SIGINT...exiting.\n");
    exit(EXIT_FAILURE);
  }

  if (SIG_ERR == signal(SIGTERM, _sig_handler)) {
    fprintf(stderr, "Can't catch SIGTERM...exiting.\n");
    exit(EXIT_FAILURE);
  }

  // Parse and set command line arguments
  while ((option_char = getopt_long(argc, argv, "p:d:rhm:t:", gLongOptions,
                                    NULL)) != -1) {
    switch (option_char) {
      case 't':  /* nthreads */ 
        nthreads = atoi(optarg);
        break;
      case 'p':  /* listen-port */
        port = atoi(optarg);
        break;
      case 'm':  /* file-path */ 
        content_map = optarg;
        break;
      default:
        fprintf(stderr, "%s", USAGE);
        exit(1);
      case 'd':  /* delay */
        content_delay = (unsigned long int)atoi(optarg);
        break;
      case 'h':  /* help */ 
        fprintf(stdout, "%s", USAGE);
        exit(0);
        break;
    }
  }

  /* not useful, but it ensures the initial code builds without warnings */
  if (nthreads < 1) {
    nthreads = 1;
  }

  if (content_delay > 5000000) {
    fprintf(stderr, "Content delay must be less than 5000000 (microseconds)\n");
    exit(__LINE__);
  }

  content_init(content_map);

  /* Initialize thread management */

  /*Initializing server*/
  gfs = gfserver_create();

  //Setting options
  gfserver_set_port(&gfs, port);
  gfserver_set_maxpending(&gfs, 86);
  gfserver_set_handler(&gfs, gfs_handler);
  gfserver_set_handlerarg(&gfs, NULL);  // doesn't have to be NULL!


  request_queue = (steque_t *)malloc(sizeof(steque_t));
  steque_init(request_queue);
  //mutex and cond init
  pthread_mutex_init(&queue_mutex, NULL);
  pthread_cond_init(&queue_populated_cond, NULL);


  pthread_t thread_pool[nthreads];
  int ret;
  for (int i = 0; i < nthreads; i++) {
    ret = pthread_create(&thread_pool[i], NULL, thread_func, &i);
    if (ret < 0) {
      printf("thread %i create error", i);
    }
  }

  /*Loops forever*/
  gfserver_serve(&gfs);
  //free resources
  
  steque_destroy(request_queue);
  free(request_queue);
  pthread_mutex_destroy(&queue_mutex);
  pthread_cond_destroy(&queue_populated_cond);
  content_destroy();
}

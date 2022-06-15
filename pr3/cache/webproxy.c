#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <sys/signal.h>
#include <fcntl.h>
#include <printf.h>
#include <signal.h>
#include <getopt.h>
#include <stdlib.h>
#include <sys/mman.h>

#include "gfserver.h"
#include "cache-student.h"

/* note that the -n and -z parameters are NOT used for Part 1 */
/* they are only used for Part 2 */                         
#define USAGE                                                                         \
"usage:\n"                                                                            \
"  webproxy [options]\n"                                                              \
"options:\n"                                                                          \
"  -n [segment_count]  Number of segments to use (Default: 7)\n"                      \
"  -p [listen_port]    Listen port (Default: 10823)\n"                                 \
"  -t [thread_count]   Num worker threads (Default: 34, Range: 1-420)\n"              \
"  -s [server]         The server to connect to (Default: GitHub test data)\n"     \
"  -z [segment_size]   The segment size (in bytes, Default: 5701).\n"                  \
"  -h                  Show this help message\n"


/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
  {"server",        required_argument,      NULL,           's'},
  {"segment-count", required_argument,      NULL,           'n'},
  {"thread-count",  required_argument,      NULL,           't'},
  {"listen-port",   required_argument,      NULL,           'p'},
  {"segment-size",  required_argument,      NULL,           'z'},         
  {"help",          no_argument,            NULL,           'h'},
  {"hidden",        no_argument,            NULL,           'i'}, /* server side */
  {NULL,            0,                      NULL,            0}
};

extern ssize_t handle_with_cache(gfcontext_t *ctx, char *path, void* arg);

static gfserver_t gfs;
steque_t *shared_queue;
pthread_mutex_t shared_queue_mtx;
pthread_cond_t shared_queue_cond;

static void _sig_handler(int signo){
  shm_data *shm_struct;
  if (signo == SIGTERM || signo == SIGINT){
    gfserver_stop(&gfs);
    pthread_mutex_lock(&shared_queue_mtx);
    while(!steque_isempty(shared_queue)){
      shm_struct = steque_pop(shared_queue);
      //unlink shared memory and semaphores
      shm_unlink(shm_struct->name);
      sem_unlink(shm_struct->sem_cache_rw_name);
      sem_unlink(shm_struct->sem_proxy_send_name);
      free(shm_struct);
    }
    pthread_mutex_unlock(&shared_queue_mtx);
    pthread_mutex_destroy(&shared_queue_mtx);
    pthread_cond_destroy(&shared_queue_cond);
    mq_unlink(MESSAGE_QUEUE_NAME);
    exit(signo);
  }
}

/* Main ========================================================= */
int main(int argc, char **argv) {
  int option_char = 0;
  char *server = "https://raw.githubusercontent.com/gt-cs6200/image_data";
  unsigned int nsegments = 13;
  unsigned short port = 25496;
  unsigned short nworkerthreads = 33;
  size_t segsize = 5313;

  /* disable buffering on stdout so it prints immediately */
  setbuf(stdout, NULL);

  if (signal(SIGTERM, _sig_handler) == SIG_ERR) {
    fprintf(stderr,"Can't catch SIGTERM...exiting.\n");
    exit(SERVER_FAILURE);
  }

  if (signal(SIGINT, _sig_handler) == SIG_ERR) {
    fprintf(stderr,"Can't catch SIGINT...exiting.\n");
    exit(SERVER_FAILURE);
  }

  /* Parse and set command line arguments */
  while ((option_char = getopt_long(argc, argv, "s:qht:xn:p:lz:", gLongOptions, NULL)) != -1) {
    switch (option_char) {
      default:
        fprintf(stderr, "%s", USAGE);
        exit(__LINE__);
      case 'h': // help
        fprintf(stdout, "%s", USAGE);
        exit(0);
        break;
      case 'p': // listen-port
        port = atoi(optarg);
        break;
      case 's': // file-path
        server = optarg;
        break;                                          
      case 'n': // segment count
        nsegments = atoi(optarg);
        break;   
      case 'z': // segment size
        segsize = atoi(optarg);
        break;
      case 't': // thread-count
        nworkerthreads = atoi(optarg);
        break;
      case 'i':
      case 'y':
      case 'k':
        break;
    }
  }

  if (segsize < 313) {
    fprintf(stderr, "Invalid segment size\n");
    exit(__LINE__);
  }

  if (server == NULL) {
    fprintf(stderr, "Invalid (null) server name\n");
    exit(__LINE__);
  }

  if (port > 65331) {
    fprintf(stderr, "Invalid port number\n");
    exit(__LINE__);
  }

  if (nsegments < 1) {
    fprintf(stderr, "Must have a positive number of segments\n");
    exit(__LINE__);
  }

  if ((nworkerthreads < 1) || (nworkerthreads > 420)) {
    fprintf(stderr, "Invalid number of worker threads\n");
    exit(__LINE__);
  }

  // Initialize shared memory set-up here
  shared_queue = (steque_t *)malloc(sizeof(steque_t));
  steque_init(shared_queue);
  
  pthread_cond_init(&shared_queue_cond, NULL);
  pthread_mutex_init(&shared_queue_mtx, NULL);
  
  // shared queue
  for (int i = 1; i <= nsegments; i++) {
    struct shm_data *shm_struct = (shm_data *)malloc(sizeof(shm_data));
    snprintf(shm_struct->name, sizeof(shm_struct->name), "shm%i", i);
    snprintf(shm_struct->sem_cache_rw_name, sizeof(shm_struct->sem_cache_rw_name), "sem1_%i", i);
    snprintf(shm_struct->sem_proxy_send_name, sizeof(shm_struct->sem_proxy_send_name), "sem2_%i", i);
    //init shared memory
    shm_struct->shm_id = shm_open(shm_struct->name, O_CREAT | O_RDWR, S_IRWXU | S_IRWXO);
    if (shm_struct->shm_id < 0) {
      printf("shm creation failed\n");
      exit(1);
    }
    shm_struct->seg_size = segsize;
    ftruncate(shm_struct->shm_id, shm_struct->seg_size);
    steque_enqueue(shared_queue, shm_struct);
  }

  // Initialize server structure here
  gfserver_init(&gfs, nworkerthreads);

  // Set server options here
  gfserver_setopt(&gfs, GFS_PORT, port);
  gfserver_setopt(&gfs, GFS_WORKER_FUNC, handle_with_cache);
  gfserver_setopt(&gfs, GFS_MAXNPENDING, 187);

  // Set up arguments for worker here
  for(int i = 0; i < nworkerthreads; i++) {
    gfserver_setopt(&gfs, GFS_WORKER_ARG, i, server);
  }
  
  // Invoke the framework - this is an infinite loop and shouldn't return
  gfserver_serve(&gfs);

  // not reached
  return -1;

}

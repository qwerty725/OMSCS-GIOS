#include <stdlib.h>

#include "gfclient-student.h"

#define MAX_THREADS (1024)
#define PATH_BUFFER_SIZE 1221

#define USAGE                                                    \
  "usage:\n"                                                     \
  "  webclient [options]\n"                                      \
  "options:\n"                                                   \
  "  -h                  Show this help message\n"               \
  "  -s [server_addr]    Server address (Default: 127.0.0.1)\n"  \
  "  -p [server_port]    Server port (Default: 10823)\n"         \
  "  -r [num_requests]   Request download total (Default: 21)\n" \
  "  -t [nthreads]       Number of threads (Default 12)\n"       \
  "  -w [workload_path]  Path to workload file (Default: workload.txt)\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
    {"help", no_argument, NULL, 'h'},
    {"nrequests", required_argument, NULL, 'r'},
    {"server", required_argument, NULL, 's'},
    {"port", required_argument, NULL, 'p'},
    {"workload-path", required_argument, NULL, 'w'},
    {"nthreads", required_argument, NULL, 't'},
    {NULL, 0, NULL, 0}};

static void Usage() { fprintf(stderr, "%s", USAGE); }

static void localPath(char *req_path, char *local_path) {
  static int counter = 0;

  sprintf(local_path, "%s-%06d", &req_path[1], counter++);
}

pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t queue_populated_cond = PTHREAD_COND_INITIALIZER;
steque_t *request_queue;

int reqs_finished = 0;
int nrequests = 8;
int nthreads = 20;
char *server = "localhost";
unsigned short port = 10823;


static FILE *openFile(char *path) {
  char *cur, *prev;
  FILE *ans;

  /* Make the directory if it isn't there */
  prev = path;
  while (NULL != (cur = strchr(prev + 1, '/'))) {
    *cur = '\0';

    if (0 > mkdir(&path[0], S_IRWXU)) {
      if (errno != EEXIST) {
        perror("Unable to create directory");
        exit(EXIT_FAILURE);
      }
    }

    *cur = '/';
    prev = cur;
  }

  if (NULL == (ans = fopen(&path[0], "w"))) {
    perror("Unable to open file");
    exit(EXIT_FAILURE);
  }

  return ans;
}

/* Callbacks ========================================================= */
static void writecb(void *data, size_t data_len, void *arg) {
  FILE *file = (FILE *)arg;

  fwrite(data, 1, data_len, file);
}


/* Main ========================================================= */
int main(int argc, char **argv) {
  /* COMMAND LINE OPTIONS ============================================= */
  char *workload_path = "workload.txt";
  int option_char = 0;
  char *req_path = NULL;
  void *join_status;

  setbuf(stdout, NULL);  // disable caching

  // Parse and set command line arguments
  while ((option_char = getopt_long(argc, argv, "p:n:hs:t:r:w:", gLongOptions,
                                    NULL)) != -1) {
    switch (option_char) {
      case 'r':
        nrequests = atoi(optarg);
        break;
      case 'n':  // nrequests
        break;
      default:
        Usage();
        exit(1);
      case 'p':  // port
        port = atoi(optarg);
        break;
      case 'h':  // help
        Usage();
        exit(0);
        break;
      case 's':  // server
        server = optarg;
        break;
      case 't':  // nthreads
        nthreads = atoi(optarg);
        break;
      case 'w':  // workload-path
        workload_path = optarg;
        break;
    }
  }

  if (EXIT_SUCCESS != workload_init(workload_path)) {
    fprintf(stderr, "Unable to load workload file %s.\n", workload_path);
    exit(EXIT_FAILURE);
  }

  if (nthreads < 1) {
    nthreads = 1;
  }
  if (nthreads > MAX_THREADS) {
    nthreads = MAX_THREADS;
  }

  gfc_global_init();

  // add your threadpool creation here 
  request_queue = (steque_t *)malloc(sizeof(steque_t));
  steque_init(request_queue);
  pthread_mutex_init(&queue_mutex, NULL);
  pthread_cond_init(&queue_populated_cond, NULL);
  //trim nthreads if not needed so join can finish
  if (nrequests < nthreads) {
    nthreads = nrequests;
  }
  pthread_t thread_pool[nthreads];
  int ret;
  for (int i = 0; i < nthreads; i++) {
    ret = pthread_create(&thread_pool[i], NULL, thread_func, &i);
    if (ret < 0) {
      printf("thread %i create error", i);
    }
  }

  /* Build your queue of requests here */
  for (int i = 0; i < nrequests; i++) {
    /* Note that when you have a worker thread pool, you will need to move this
     * logic into the worker threads */
    req_path = workload_get_path();

    if (strlen(req_path) > 1024) {
      fprintf(stderr, "Request path exceeded maximum of 1024 characters\n.");
      exit(EXIT_FAILURE);
    }


    pthread_mutex_lock(&queue_mutex);
    steque_enqueue(request_queue, req_path);

    pthread_cond_broadcast(&queue_populated_cond);
    pthread_mutex_unlock(&queue_mutex);
  }

  for (int i = 0; i < nthreads; i++) {
    pthread_join(thread_pool[i], &join_status);
  }

  steque_destroy(request_queue);
  free(request_queue);
  pthread_mutex_destroy(&queue_mutex);
  pthread_cond_destroy(&queue_populated_cond);

  gfc_global_cleanup();  /* use for any global cleanup for AFTER your thread
                          pool has terminated. */

  return 0;
}


void *thread_func(void *arg) {
  char *req_path = NULL;
  int returncode = 0;
  gfcrequest_t *gfr = NULL;
  FILE *file = NULL;
  char local_path[PATH_BUFFER_SIZE];

  while(1) {
    pthread_mutex_lock(&queue_mutex);
    while (steque_isempty(request_queue)) {
      pthread_cond_wait(&queue_populated_cond, &queue_mutex);
    }
    req_path = steque_pop(request_queue);
    pthread_mutex_unlock(&queue_mutex);
    
    //reused from previous main()
    localPath(req_path, local_path);

    file = openFile(local_path);

    gfr = gfc_create();
    gfc_set_server(&gfr, server);
    gfc_set_path(&gfr, req_path);
    gfc_set_port(&gfr, port);
    gfc_set_writefunc(&gfr, writecb);
    gfc_set_writearg(&gfr, file);

    fprintf(stdout, "Requesting %s%s\n", server, req_path);

    if (0 > (returncode = gfc_perform(&gfr))) {
      fprintf(stdout, "gfc_perform returned an error %d\n", returncode);
      fclose(file);
      if (0 > unlink(local_path))
        fprintf(stderr, "warning: unlink failed on %s\n", local_path);
    } else {
      fclose(file);
    }

    if (gfc_get_status(&gfr) != GF_OK) {
      if (0 > unlink(local_path))
        fprintf(stderr, "warning: unlink failed on %s\n", local_path);
    }

    fprintf(stdout, "Status: %s\n", gfc_strstatus(gfc_get_status(&gfr)));
    fprintf(stdout, "Received %zu of %zu bytes\n", gfc_get_bytesreceived(&gfr),
            gfc_get_filelen(&gfr));

    gfc_cleanup(&gfr);

    reqs_finished += 1;
    if (nrequests-nthreads < reqs_finished) {
      break;
    }
  }
  pthread_exit(NULL);
}
  

    /*
     * note that when you move the above logic into your worker thread, you will
     * need to coordinate with the boss thread here to effect a clean shutdown.
     */
  
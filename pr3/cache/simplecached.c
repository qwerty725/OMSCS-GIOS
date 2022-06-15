#include <stdio.h>
#include <unistd.h>
#include <printf.h>
#include <string.h>
#include <signal.h>
#include <limits.h>
#include <sys/signal.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>

#include "gfserver.h"
#include "cache-student.h"
#include "simplecache.h"
#include <semaphore.h>

#define MAX_CACHE_REQUEST_LEN 500

#if !defined(CACHE_FAILURE)
    #define CACHE_FAILURE (-1)
#endif // CACHE_FAILURE

pthread_mutex_t req_struct_queue_mtx;
pthread_cond_t req_struct_queue_cond;
steque_t *req_struct_queue;


static void _sig_handler(int signo){
	if (signo == SIGTERM || signo == SIGINT){
		// you should do IPC cleanup here
		//mq_close(MESSAGE_QUEUE_NAME);
		//pthread_mutex_destroy(&req_struct_queue_mtx);
		//pthread_cond_destroy(&req_struct_queue_cond);
		simplecache_destroy();
		exit(signo);
	}
}

unsigned long int cache_delay;

#define USAGE                                                                 \
"usage:\n"                                                                    \
"  simplecached [options]\n"                                                  \
"options:\n"                                                                  \
"  -c [cachedir]       Path to static files (Default: ./)\n"                  \
"  -t [thread_count]   Thread count for work queue (Default is 42, Range is 1-235711)\n"      \
"  -d [delay]          Delay in simplecache_get (Default is 0, Range is 0-2500000 (microseconds)\n "	\
"  -h                  Show this help message\n"

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
  {"cachedir",           required_argument,      NULL,           'c'},
  {"help",               no_argument,            NULL,           'h'},
  {"nthreads",           required_argument,      NULL,           't'},
  {"hidden",			 no_argument,			 NULL,			 'i'}, /* server side */
  {"delay", 			 required_argument,		 NULL, 			 'd'}, // delay.
  {NULL,                 0,                      NULL,             0}
};

void Usage() {
  fprintf(stdout, "%s", USAGE);
}
void *worker_func(void *arg) {
	cache_data *req_struct;
	int shm_id;
	void *shared_mem;
	int fd;
	int bytes_transferred = 0;
	int bytes_read;
	struct stat statbuf;
	size_t file_len;
	sem_t *sem_cache_rw, *sem_proxy_send;
	while(1) {
		//printf("before mutex\n");
		pthread_mutex_lock(&req_struct_queue_mtx);
		while(steque_isempty(req_struct_queue))  {
			pthread_cond_wait(&req_struct_queue_cond, &req_struct_queue_mtx);
		}
		req_struct = (struct cache_data *)steque_pop(req_struct_queue);
		pthread_mutex_unlock(&req_struct_queue_mtx);
		//printf("worker %i %s %s %s %i %s\n", req_struct->shm_id, req_struct->name, req_struct->sem_cache_rw_name, req_struct->sem_proxy_send_name, req_struct->seg_size, req_struct->path);
		//printf("aftermutex\n");
		//open shared memory to proxy
		shm_id = shm_open(req_struct->name, O_RDWR, S_IRWXU | S_IRWXO);
		if (shm_id<0) {
			printf("shm open fail");
		}
		shared_mem = mmap(NULL, req_struct->seg_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_id, 0);
		//get file descriptor from cache		
		fd = simplecache_get(req_struct->path);
		//check for invalid file path
		if (fd < 0){
			//printf("invalid fd\n");
			file_len = 0;
			//send -1 over shared mem to tell proxy that path is invalid and send FNF
			sprintf(shared_mem,"%d", -1);
		}
		else {
			//printf("valid fd\n");
			fstat(fd, &statbuf);
			file_len = (size_t) statbuf.st_size;
			//send file size over shared mem to tell proxy how many bytes to expect
			sprintf(shared_mem, "%ld", file_len);
		}


		//open semaphores from structure sem name field
		sem_cache_rw = sem_open(req_struct->sem_cache_rw_name, O_CREAT | O_RDWR, 0644, 0);
		sem_proxy_send = sem_open(req_struct->sem_proxy_send_name, O_CREAT | O_RDWR, 0644, 0);
		if (sem_cache_rw == SEM_FAILED || sem_proxy_send == SEM_FAILED) {
			printf("semaphore open failed");
		}
		//post first semaphore to indicate file size has be entered into shared mem
		sem_post(sem_cache_rw);
		bytes_transferred = 0;
		while (bytes_transferred < file_len) {
			//wait for proxy semaphore to indicate header has been sent
			sem_wait(sem_proxy_send);
			//read bytes into shared memory
			bytes_read = pread(fd,shared_mem,req_struct->seg_size,bytes_transferred);
			if (bytes_read < 0){
				printf("pread error\n");
				exit(1);
			}
			bytes_transferred += bytes_read;
			//indicate to proxy that data is entered into shared mem
			sem_post(sem_cache_rw);
		}
		sem_close(sem_cache_rw);
		sem_close(sem_proxy_send);
		munmap(shared_mem, req_struct->seg_size);
		free(req_struct);
	}
}
int main(int argc, char **argv) {
	int nthreads = 11;
	char *cachedir = "locals.txt";
	char option_char;
	struct mq_attr attr;
	mqd_t request_queue;
	char request[MAX_CACHE_REQUEST_LEN];
	char *token;
	attr.mq_flags = 0;
	attr.mq_maxmsg = 10;
	attr.mq_msgsize = MAX_CACHE_REQUEST_LEN;
	attr.mq_curmsgs = 0;

	/* disable buffering to stdout */
	setbuf(stdout, NULL);

	while ((option_char = getopt_long(argc, argv, "d:ic:hlt:x", gLongOptions, NULL)) != -1) {
		switch (option_char) {
			default:
				Usage();
				exit(1);
			case 'h': // help
				Usage();
				exit(0);
				break;    
			case 't': // thread-count
				nthreads = atoi(optarg);
				break;
            case 'c': //cache directory
				cachedir = optarg;
				break;
            case 'd':
				cache_delay = (unsigned long int) atoi(optarg);
				break;
			case 'i': // server side usage
			case 'u': // experimental
			case 'j': // experimental
				break;
		}
	}

	if (cache_delay > 2500000) {
		fprintf(stderr, "Cache delay must be less than 2500000 (us)\n");
		exit(__LINE__);
	}

	if ((nthreads>211803) || (nthreads < 1)) {
		fprintf(stderr, "Invalid number of threads must be in between 1-235711\n");
		exit(__LINE__);
	}

	if (SIG_ERR == signal(SIGTERM, _sig_handler)){
		fprintf(stderr,"Unable to catch SIGTERM...exiting.\n");
		exit(CACHE_FAILURE);
	}

	if (SIG_ERR == signal(SIGINT, _sig_handler)){
		fprintf(stderr,"Unable to catch SIGINT...exiting.\n");
		exit(CACHE_FAILURE);
	}

	// Initialize cache
	simplecache_init(cachedir);
	//printf("init cached\n");
	req_struct_queue = (steque_t *)malloc(sizeof(steque_t));
	steque_init(req_struct_queue);
	
	pthread_cond_init(&req_struct_queue_cond, NULL);
	pthread_mutex_init(&req_struct_queue_mtx, NULL);
	//printf("mutexes\n");
	//worker pool
	pthread_t worker_pool[nthreads];
	int ret;
	for (int i=0; i<nthreads; i++) {
		ret = pthread_create(&worker_pool[i], NULL, worker_func, NULL);
		if (ret<0) {
			printf("fail to create worker thread\n");
			exit(1);
		}

	}
	//printf("threads\n");
	//open message queue
	request_queue = mq_open(MESSAGE_QUEUE_NAME, O_CREAT | O_RDWR, 0644, &attr);
	if (request_queue == -1) {
		printf("cache message queue fail\n");
		exit(1);
	}
	//printf("message queue\n");
	while(1) {
		//printf("before receive\n");
		//process request from request message queue
		if(mq_receive(request_queue, request, MAX_CACHE_REQUEST_LEN, NULL) < 0) {
			printf("message queue receive fail\n");
		}
		//id name sem1name sem2name segsize path
		cache_data *req_struct;
		req_struct = malloc(sizeof(cache_data));
		char *ptr = request;
		//printf("%s\n", request);
		token = strtok_r(request, " ", &ptr);
		req_struct->shm_id = atoi(token);
		token = strtok_r(NULL, " ", &ptr);
		sprintf(req_struct->name, "%s", token);
		token = strtok_r(NULL, " ", &ptr);
		sprintf(req_struct->sem_cache_rw_name, "%s", token);
		token = strtok_r(NULL, " ", &ptr);
		sprintf(req_struct->sem_proxy_send_name, "%s", token);
		token = strtok_r(NULL, " ", &ptr);
		req_struct->seg_size = atoi(token);
		token = strtok_r(NULL, " ", &ptr);
		sprintf(req_struct->path, "%s", token);
		//printf("%i %s %s %s %i %s\n", req_struct->shm_id, req_struct->name, req_struct->sem_cache_rw_name, req_struct->sem_proxy_send_name, req_struct->seg_size, req_struct->path);
		//enter struct into queue
		pthread_mutex_lock(&req_struct_queue_mtx);
		steque_enqueue(req_struct_queue, req_struct);
		//printf("queued\n");
		pthread_mutex_unlock(&req_struct_queue_mtx);
		pthread_cond_broadcast(&req_struct_queue_cond);
		
		//printf("cache completed %s\n",req_struct->path);
	}
	// Won't execute
	return 0;
}


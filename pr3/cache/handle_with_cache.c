#include "gfserver.h"
#include "cache-student.h"
#include <time.h>
#define BUFSIZE (357)

#define POLL_TIME ((struct timespec){0, 500000000})
/*
 * Placeholder demonstrates use of gfserver library, replace with your
 * own implementation and any other functions you may need.
 */
ssize_t handle_with_cache(gfcontext_t *ctx, const char *path, void* arg){
    int bytes_transferred, bytes_sent;
	char *request = malloc(MAX_CACHE_REQUEST_LEN+10);
	mqd_t request_queue;
	struct shm_data *shm_struct;
	int status;
	//printf("%s\n",path);
	//pop shared memory info struct from queue
	pthread_mutex_lock(&shared_queue_mtx);
	while (steque_isempty(shared_queue)) {
		pthread_cond_wait(&shared_queue_cond, &shared_queue_mtx);
	}
	shm_struct = steque_pop(shared_queue);
	pthread_mutex_unlock(&shared_queue_mtx);
	//map shared memory
	shm_struct->data = mmap(NULL, shm_struct->seg_size, PROT_READ|PROT_WRITE, MAP_SHARED, shm_struct->shm_id, 0);
	//init semaphores
	shm_struct->sem_cache_rw = sem_open(shm_struct->sem_cache_rw_name, O_CREAT | O_RDWR, 0644, 0);
	shm_struct->sem_proxy_send = sem_open(shm_struct->sem_proxy_send_name, O_CREAT | O_RDWR, 0644, 0);

	//open request message queue
	request_queue = mq_open(MESSAGE_QUEUE_NAME, O_WRONLY);
	while (request_queue == -1) {
		//pause .5 sec
		nanosleep(&POLL_TIME, NULL);
		request_queue = mq_open(MESSAGE_QUEUE_NAME, O_WRONLY);
	}
	//printf(MESSAGE_QUEUE_NAME);
	//id name sem1name sem2name segsize path
	//printf("%d\n", MAX_CACHE_REQUEST_LEN);
	snprintf(request, MAX_CACHE_REQUEST_LEN, "%d %s %s %s %d %s", shm_struct->shm_id, shm_struct->name, shm_struct->sem_cache_rw_name, shm_struct->sem_proxy_send_name, shm_struct->seg_size, path);
	//printf("%s\n", request);
	status = mq_send(request_queue, request, MAX_CACHE_REQUEST_LEN, 0);
	if (status < 0) {
		perror("mq send error\n");
	}
	free(request);
	//printf("%s - sem1.\n",shm_struct->sem_cache_rw_name);
	//wait for cache to receive request and get fd and enter file size into shm
	sem_wait(shm_struct->sem_cache_rw);

	//receive file size from shared mem
	shm_struct->size = atoi(shm_struct->data);

	if (shm_struct->size < 0){
		//FNF
		printf("FNF\n");
		bytes_sent = gfs_sendheader(ctx, GF_FILE_NOT_FOUND, 0);
	}
	else {
		printf("File size: %d\n", shm_struct->size);
		bytes_sent = gfs_sendheader(ctx, GF_OK, shm_struct->size);
	}

	if (bytes_sent < 0) {
		printf("gfs send header fail\n");
		return SERVER_FAILURE;
	}

	bytes_transferred = 0;

	while (bytes_transferred < shm_struct->size){
		//increment header sent sem for first loop, indicate data send for rest of loops
		sem_post(shm_struct->sem_proxy_send);
		//wait for shm to fill
		sem_wait(shm_struct->sem_cache_rw);
		if (shm_struct->size - bytes_transferred < shm_struct->seg_size) {
			bytes_sent = gfs_send(ctx, shm_struct->data, shm_struct->size - bytes_transferred);
		}
		else {
			bytes_sent = gfs_send(ctx, shm_struct->data, shm_struct->seg_size);
		}

		//send fail check
		if (bytes_sent < 0) {
			printf("gfs send fail\n");
			return SERVER_FAILURE;
		}
		else{
			bytes_transferred += bytes_sent;
		}
	}
	//free semaphores
	sem_close(shm_struct->sem_cache_rw);
	sem_close(shm_struct->sem_proxy_send);
	sem_unlink(shm_struct->sem_cache_rw_name);
	sem_unlink(shm_struct->sem_proxy_send_name);

	//unmap shared mem
	munmap(shm_struct->data, shm_struct->seg_size);

	//return struct to the queue
	pthread_mutex_lock(&shared_queue_mtx);
	steque_enqueue(shared_queue, shm_struct);
	pthread_mutex_unlock(&shared_queue_mtx);
	pthread_cond_broadcast(&shared_queue_cond);
	printf("proxy bytes_transferred %i\n", bytes_transferred);
	return bytes_transferred;
}


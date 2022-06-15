#include "gfserver-student.h"
#include "gfserver.h"
#include "content.h"
#include "workload.h"
#include <stdlib.h>
#include <stdio.h>

extern pthread_mutex_t queue_mutex;
extern pthread_cond_t queue_populated_cond;
extern steque_t *request_queue;
//
//  The purpose of this function is to handle a get request
//
//  The ctx is a pointer to the "context" operation and it contains connection state
//  The path is the path being retrieved
//  The arg allows the registration of context that is passed into this routine.
//  Note: you don't need to use arg. The test code uses it in some cases, but
//        not in others.
//
gfh_error_t gfs_handler(gfcontext_t **ctx, const char *path, void* arg){
	request_t *request = (request_t *)malloc(sizeof(request_t));
	request->ctx = *ctx;
	request->path = path;

	if (request == NULL) {
		printf("request did not initialize\n");
		return gfh_failure;
	}

	pthread_mutex_lock(&queue_mutex);
	steque_enqueue(request_queue, request);

	*ctx = NULL;
	pthread_cond_broadcast(&queue_populated_cond);
	pthread_mutex_unlock(&queue_mutex);
	return gfh_success;
}


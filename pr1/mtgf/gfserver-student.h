/*
 *  This file is for use by students to define anything they wish.  It is used by the gf server implementation
 */
#ifndef __GF_SERVER_STUDENT_H__
#define __GF_SERVER_STUDENT_H__

#include "gf-student.h"
#include "gfserver.h"
#include "content.h"


void init_threads(size_t numthreads);
void cleanup_threads();

// function for worker threads
void *thread_func(void *arg);

typedef struct request_t {
    const char *path;
    gfcontext_t *ctx;
} request_t;

#endif // __GF_SERVER_STUDENT_H__

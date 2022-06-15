#include <sys/stat.h>
#include <fcntl.h> 
#include <semaphore.h>
#include <mqueue.h>
#include <sys/mman.h>
#include <sys/stat.h> /* For mode constants */
#include <fcntl.h>    /* For O_* constants */


#define MAX_CACHE_REQUEST_LEN 500
#define MESSAGE_QUEUE_NAME "/message_queue"
/* In case you want to implement the shared memory IPC as a library... */
extern steque_t *shared_queue;
extern pthread_mutex_t shared_queue_mtx;
extern pthread_cond_t shared_queue_cond;

typedef struct shm_data
{
    sem_t *sem_cache_rw;
    sem_t *sem_proxy_send;
    char name[10];
    char sem_cache_rw_name[10];
    char sem_proxy_send_name[10];
    void *data;
    int shm_id;
    int seg_size;
    int size;
}shm_data;

typedef struct cache_data
{
    char name[10];
    char sem_cache_rw_name[10];
    char sem_proxy_send_name[10];
    int shm_id;
    int seg_size;
    char path[256];
}cache_data;
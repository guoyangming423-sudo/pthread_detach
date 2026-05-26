#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <pthread.h>

/* ---- task_t — a unit of work ---- */
typedef struct {
    void (*func)(void *);
    void *arg;
} task_t;

/* ---- task_queue_t — bounded ring buffer ---- */
typedef struct {
    task_t         *tasks;
    int             head;
    int             tail;
    int             count;
    int             capacity;
    int             shutdown;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} task_queue_t;

/* ---- thread_pool_t ---- */
typedef struct {
    task_queue_t    queue;
    pthread_t      *threads;
    int             running;
    int             min_threads;
    int             max_threads;
    int             idle_timeout;
    int             shutdown;
    pthread_mutex_t pool_mutex;
    pthread_cond_t  pool_cond;
} thread_pool_t;

/* ---- Public API ---- */

int  thread_pool_init(thread_pool_t *pool, int min, int max,
                      int queue_cap, int idle_sec);
int  thread_pool_submit(thread_pool_t *pool, void (*func)(void *), void *arg);
void thread_pool_shutdown_wait(thread_pool_t *pool);
void thread_pool_shutdown_now(thread_pool_t *pool);
int  thread_pool_active_threads(thread_pool_t *pool);
int  thread_pool_pending_tasks(thread_pool_t *pool);

#endif /* THREAD_POOL_H  */

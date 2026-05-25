#include "thread_pool.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>

/* ========== task_queue_t internals ========== */

static int queue_init(task_queue_t *q, int capacity)
{
    q->tasks = (task_t *) malloc(sizeof(task_t) * (size_t) capacity);
    if (!q->tasks)
        return -1;
    q->head     = 0;
    q->tail     = 0;
    q->count    = 0;
    q->capacity = capacity;
    q->shutdown = 0;
    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        free(q->tasks);
        return -1;
    }
    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&q->mutex);
        free(q->tasks);
        return -1;
    }
    if (pthread_cond_init(&q->not_full, NULL) != 0) {
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->mutex);
        free(q->tasks);
        return -1;
    }
    return 0;
}

static void queue_destroy(task_queue_t *q)
{
    free(q->tasks);
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
}

static int queue_push_nb(task_queue_t *q, task_t task)
{
    pthread_mutex_lock(&q->mutex);
    if (q->shutdown || q->count == q->capacity) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    q->tasks[q->tail] = task;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* Returns: 0 = got task, -1 = shutdown, -2 = timeout */
static int queue_pop_timeout(task_queue_t *q, task_t *task, int timeout_sec)
{
    pthread_mutex_lock(&q->mutex);
    while (q->count == 0 && !q->shutdown) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_sec;
        int rc = pthread_cond_timedwait(&q->not_empty, &q->mutex, &ts);
        if (rc == ETIMEDOUT) {
            pthread_mutex_unlock(&q->mutex);
            return -2;
        }
    }
    if (q->shutdown && q->count == 0) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    *task = q->tasks[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

static void queue_shutdown(task_queue_t *q, int clear_tasks)
{
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    if (clear_tasks) {
        q->count = 0;
        q->head  = 0;
        q->tail  = 0;
    }
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

/* ========== Worker thread ========== */

static void *worker_loop(void *arg)
{
    thread_pool_t *pool = (thread_pool_t *) arg;

    for (;;) {
        task_t task;
        int ret = queue_pop_timeout(&pool->queue, &task, pool->idle_timeout);

        if (ret == 0) {
            /* Got a task 鈥?execute it outside any lock */
            task.func(task.arg);
        } else if (ret == -2) {
            /* Timed out 鈥?consider self-reaping */
            pthread_mutex_lock(&pool->pool_mutex);
            if (pool->running > pool->min_threads && !pool->shutdown) {
                /* Remove self from threads[] */
                pthread_t me = pthread_self();
                for (int i = 0; i < pool->running; i++) {
                    if (pthread_equal(me, pool->threads[i])) {
                        pool->threads[i] = pool->threads[pool->running - 1];
                        pool->running--;
                        break;
                    }
                }
                pthread_mutex_unlock(&pool->pool_mutex);
                pthread_detach(pthread_self());
                break;  /* exit thread 鈥?detached, no zombie */
            }
            pthread_mutex_unlock(&pool->pool_mutex);
            /* min thread 鈥?loop back and keep waiting */
        } else {
            /* ret == -1: queue shutdown 鈥?exit without touching pool state */
            break;
        }
    }
    return NULL;
}

/* ========== Public API ========== */

int thread_pool_init(thread_pool_t *pool, int min, int max,
                     int queue_cap, int idle_sec)
{
    if (!pool || min < 1 || max < min || queue_cap < 1 || idle_sec < 1)
        return -1;

    memset(pool, 0, sizeof(*pool));
    pool->min_threads  = min;
    pool->max_threads  = max;
    pool->idle_timeout = idle_sec;

    if (queue_init(&pool->queue, queue_cap) != 0)
        return -1;

    pool->threads = (pthread_t *) malloc(sizeof(pthread_t) * (size_t) max);
    if (!pool->threads) {
        queue_destroy(&pool->queue);
        return -1;
    }

    if (pthread_mutex_init(&pool->pool_mutex, NULL) != 0) {
        free(pool->threads);
        queue_destroy(&pool->queue);
        return -1;
    }
    if (pthread_cond_init(&pool->pool_cond, NULL) != 0) {
        pthread_mutex_destroy(&pool->pool_mutex);
        free(pool->threads);
        queue_destroy(&pool->queue);
        return -1;
    }

    int i;
    for (i = 0; i < min; i++) {
        if (pthread_create(&pool->threads[i], NULL, worker_loop, pool) != 0) {
            fprintf(stderr, "thread_pool_init: pthread_create failed for "
                            "thread %d/%d\n", i, min);
            pool->shutdown = 1;
            queue_shutdown(&pool->queue, 1);
            for (int j = 0; j < i; j++)
                pthread_join(pool->threads[j], NULL);
            free(pool->threads);
            queue_destroy(&pool->queue);
            pthread_mutex_destroy(&pool->pool_mutex);
            pthread_cond_destroy(&pool->pool_cond);
            return -1;
        }
    }
    pthread_mutex_lock(&pool->pool_mutex);
    pool->running = min;
    pthread_mutex_unlock(&pool->pool_mutex);
    return 0;
}

int thread_pool_submit(thread_pool_t *pool, void (*func)(void *), void *arg)
{
    if (!pool || !func)
        return -1;

    /* Check shutdown before pushing to avoid TOCTOU race */
    pthread_mutex_lock(&pool->pool_mutex);
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->pool_mutex);
        return -1;
    }
    pthread_mutex_unlock(&pool->pool_mutex);

    task_t task;
    task.func = func;
    task.arg  = arg;

    if (queue_push_nb(&pool->queue, task) != 0)
        return -1;

    /* Dynamic scale-up heuristic */
    pthread_mutex_lock(&pool->pool_mutex);
    if (pool->shutdown) {
        /* Shutdown began after push 鈥?task will be drained or discarded.
         * For fire-and-forget semantics this is acceptable. */
        pthread_mutex_unlock(&pool->pool_mutex);
        return 0;  /* task was already queued */
    }

    /* Read queue.count under queue.mutex for correctness.
     * Lock ordering: pool_mutex -> queue.mutex (consistent with shutdown). */
    pthread_mutex_lock(&pool->queue.mutex);
    int pending      = pool->queue.count;
    int has_capacity = pool->running < pool->max_threads;
    pthread_mutex_unlock(&pool->queue.mutex);

    if (pending > 0 && has_capacity) {
        pthread_t tid;
        if (pthread_create(&tid, NULL, worker_loop, pool) == 0) {
            pool->threads[pool->running] = tid;
            pool->running++;
        } else {
            fprintf(stderr, "thread_pool_submit: pthread_create failed\n");
        }
    }
    pthread_mutex_unlock(&pool->pool_mutex);
    return 0;
}

void thread_pool_shutdown_wait(thread_pool_t *pool)
{
    if (!pool)
        return;

    pthread_mutex_lock(&pool->pool_mutex);
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->pool_mutex);
        return;
    }
    pool->shutdown = 1;
    pthread_mutex_unlock(&pool->pool_mutex);

    /* Wake all workers. They see shutdown flag in queue_pop_timeout,
     * drain remaining tasks, then exit. Workers do NOT self-remove
     * from threads[] on shutdown path, so pool->running is stable. */
    queue_shutdown(&pool->queue, 0);  /* preserve remaining tasks */

    for (int i = 0; i < pool->running; i++)
        pthread_join(pool->threads[i], NULL);

    free(pool->threads);
    queue_destroy(&pool->queue);
    pthread_mutex_destroy(&pool->pool_mutex);
    pthread_cond_destroy(&pool->pool_cond);
}

void thread_pool_shutdown_now(thread_pool_t *pool)
{
    if (!pool)
        return;

    pthread_mutex_lock(&pool->pool_mutex);
    if (pool->shutdown) {
        pthread_mutex_unlock(&pool->pool_mutex);
        return;
    }
    pool->shutdown = 1;
    pthread_mutex_unlock(&pool->pool_mutex);

    queue_shutdown(&pool->queue, 1);  /* discard remaining tasks */

    for (int i = 0; i < pool->running; i++)
        pthread_join(pool->threads[i], NULL);

    free(pool->threads);
    queue_destroy(&pool->queue);
    pthread_mutex_destroy(&pool->pool_mutex);
    pthread_cond_destroy(&pool->pool_cond);
}

int thread_pool_active_threads(thread_pool_t *pool)
{
    if (!pool)
        return -1;
    pthread_mutex_lock(&pool->pool_mutex);
    int n = pool->running;
    pthread_mutex_unlock(&pool->pool_mutex);
    return n;
}

int thread_pool_pending_tasks(thread_pool_t *pool)
{
    if (!pool)
        return -1;
    pthread_mutex_lock(&pool->queue.mutex);
    int n = pool->queue.count;
    pthread_mutex_unlock(&pool->queue.mutex);
    return n;
}
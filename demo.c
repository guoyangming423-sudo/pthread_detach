#include "thread_pool.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

/* ---- helpers ---- */

static int g_counter = 0;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

static void increment(void *arg)
{
    (void) arg;
    pthread_mutex_lock(&g_mutex);
    g_counter++;
    pthread_mutex_unlock(&g_mutex);
}

static void sleep_task(void *arg)
{
    int ms = *(int *) arg;
    usleep((unsigned) ms * 1000);
}

/* ---- test runners ---- */

static void test_basic_submit_and_shutdown_wait(void)
{
    printf("TEST: basic submit + shutdown_wait ... ");
    g_counter = 0;
    thread_pool_t pool;
    int rc = thread_pool_init(&pool, 2, 4, 64, 2);
    assert(rc == 0);

    for (int i = 0; i < 100; i++)
        thread_pool_submit(&pool, increment, NULL);

    thread_pool_shutdown_wait(&pool);
    assert(g_counter == 100);
    printf("PASS (counter=%d)\n", g_counter);
}

static void test_queue_full_returns_error(void)
{
    printf("TEST: queue full returns -1 ... ");
    thread_pool_t pool;
    int rc = thread_pool_init(&pool, 1, 2, 4, 5);
    assert(rc == 0);

    int failures = 0;
    int sleep_ms = 200;
    for (int i = 0; i < 20; i++) {
        if (thread_pool_submit(&pool, sleep_task, &sleep_ms) != 0)
            failures++;
    }
    assert(failures > 0);

    thread_pool_shutdown_now(&pool);
    printf("PASS (rejected=%d)\n", failures);
}

static void test_shutdown_now_discards_tasks(void)
{
    printf("TEST: shutdown_now discards tasks ... ");
    g_counter = 0;
    thread_pool_t pool;
    int rc = thread_pool_init(&pool, 1, 2, 8, 30);
    assert(rc == 0);

    int sleep_ms = 500;
    thread_pool_submit(&pool, sleep_task, &sleep_ms);
    for (int i = 0; i < 5; i++)
        thread_pool_submit(&pool, increment, NULL);

    thread_pool_shutdown_now(&pool);
    assert(g_counter < 5);
    printf("PASS (executed=%d, expected < 5)\n", g_counter);
}

static void test_dynamic_scaling(void)
{
    printf("TEST: dynamic scaling up and down ... ");
    thread_pool_t pool;
    int rc = thread_pool_init(&pool, 1, 8, 128, 2);
    assert(rc == 0);

    int sleep_ms = 100;
    for (int i = 0; i < 50; i++)
        thread_pool_submit(&pool, sleep_task, &sleep_ms);

    usleep(100 * 1000);
    int peak = thread_pool_active_threads(&pool);
    assert(peak > pool.min_threads);
    assert(peak <= pool.max_threads);

    usleep(500 * 1000);
    sleep(4);

    int after = thread_pool_active_threads(&pool);
    assert(after <= peak);

    thread_pool_shutdown_wait(&pool);
    printf("PASS (peak=%d, after_idle=%d)\n", peak, after);
}

static void test_min_max_bounds(void)
{
    printf("TEST: min/max bounds ... ");
    thread_pool_t pool;
    int rc = thread_pool_init(&pool, 2, 4, 32, 2);
    assert(rc == 0);

    int initial = thread_pool_active_threads(&pool);
    assert(initial == 2);

    int sleep_ms = 50;
    for (int i = 0; i < 200; i++)
        thread_pool_submit(&pool, sleep_task, &sleep_ms);

    usleep(200 * 1000);
    int at_load = thread_pool_active_threads(&pool);
    assert(at_load >= 2 && at_load <= 4);

    sleep(4);
    int after = thread_pool_active_threads(&pool);
    assert(after >= 2);

    thread_pool_shutdown_wait(&pool);
    printf("PASS (init=%d, peak=%d, final=%d)\n", initial, at_load, after);
}

typedef struct {
    thread_pool_t *pool;
    int            count;
} producer_arg_t;

static void *producer_thread(void *arg)
{
    producer_arg_t *pa = (producer_arg_t *) arg;
    for (int i = 0; i < pa->count; i++)
        thread_pool_submit(pa->pool, increment, NULL);
    return NULL;
}

static void test_concurrent_submit(void)
{
    printf("TEST: concurrent producers ... ");
    g_counter = 0;
    thread_pool_t pool;
    int rc = thread_pool_init(&pool, 4, 8, 256, 5);
    assert(rc == 0);

    #define N_PRODUCERS 4
    #define N_TASKS     250

    pthread_t producers[N_PRODUCERS];
    producer_arg_t args[N_PRODUCERS];

    for (int p = 0; p < N_PRODUCERS; p++) {
        args[p].pool  = &pool;
        args[p].count = N_TASKS;
        pthread_create(&producers[p], NULL,
                       producer_thread, &args[p]);
    }

    for (int p = 0; p < N_PRODUCERS; p++)
        pthread_join(producers[p], NULL);

    thread_pool_shutdown_wait(&pool);
    assert(g_counter == N_PRODUCERS * N_TASKS);

    #undef N_PRODUCERS
    #undef N_TASKS
    printf("PASS (counter=%d)\n", g_counter);
}

static void test_null_handling(void)
{
    printf("TEST: null pointer handling ... ");
    assert(thread_pool_init(NULL, 1, 2, 4, 1) == -1);
    assert(thread_pool_submit(NULL, increment, NULL) == -1);
    thread_pool_shutdown_wait(NULL);
    thread_pool_shutdown_now(NULL);
    assert(thread_pool_active_threads(NULL) == -1);
    assert(thread_pool_pending_tasks(NULL) == -1);

    thread_pool_t pool;
    assert(thread_pool_init(&pool, 0, 2, 4, 1) == -1);
    assert(thread_pool_init(&pool, 3, 2, 4, 1) == -1);
    assert(thread_pool_init(&pool, 1, 2, 0, 1) == -1);
    assert(thread_pool_init(&pool, 1, 2, 4, 0) == -1);
    printf("PASS\n");
}

/* ---- main ---- */

int main(void)
{
    printf("=== Thread Pool Demo Tests ===\n\n");
    test_null_handling();
    test_basic_submit_and_shutdown_wait();
    test_queue_full_returns_error();
    test_shutdown_now_discards_tasks();
    test_dynamic_scaling();
    test_min_max_bounds();
    test_concurrent_submit();
    printf("\n=== All tests passed ===\n");
    return 0;
}

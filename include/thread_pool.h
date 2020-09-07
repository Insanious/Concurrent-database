#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <stddef.h>
#include <pthread.h>
#include <string.h>

typedef struct thread_pool_work thread_pool_work_t;
typedef struct thread_pool thread_pool_t;
typedef void (*thread_func_t)(void *arg);

struct thread_pool
{
	thread_pool_work_t* work_first;
	thread_pool_work_t* work_last;
	pthread_mutex_t work_mutex;
	pthread_cond_t work_cond;
	pthread_cond_t working_cond;
	size_t working_count;
	size_t thread_count;
	bool stop;
};


struct thread_pool_work
{
	thread_func_t func;
	void* arg;
	struct thread_pool_work* next;
};


thread_pool_t* thread_pool_create(size_t size);
void thread_pool_destroy(thread_pool_t* pool);
bool thread_pool_add_work(thread_pool_t* pool, thread_func_t func, void* arg);
void thread_pool_wait(thread_pool_t* pool);


#endif

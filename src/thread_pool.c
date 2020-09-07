#include "thread_pool.h"


static thread_pool_work_t* thread_pool_work_create(thread_func_t func, void* arg)
{
	thread_pool_work_t* work = NULL;

	if (!func) // check if function exists
		return NULL;

	work = malloc(sizeof(*work));
	work->func = func;
	work->arg = arg;
	work->next = NULL;

	return work;
}

static void thread_pool_work_destroy(thread_pool_work_t* work)
{
	if (!work) // sanity check
		return;

	free(work);
}

static thread_pool_work_t* thread_pool_work_get(thread_pool_t* pool)
{
	thread_pool_work_t* work = NULL;

	if (!pool)						// sanity check
		return NULL;

	if (!(work = pool->work_first))	// assign work to be the first in the pool and check if it exists
		return NULL;

	pool->work_first = work->next;	// new first
	if (!work->next)				// if it was the last one in the queue
		pool->work_last = NULL;		// correct queue

	return work;
}

static void* thread_pool_worker(void* arg)
{
	thread_pool_t* pool = arg;
	thread_pool_work_t* work = NULL;

	while (true)
	{
		pthread_mutex_lock(&(pool->work_mutex)); // grab lock

		while (!pool->work_first && !pool->stop) // wait until there is work to be done
			pthread_cond_wait(&(pool->work_cond), &(pool->work_mutex));

		if (pool->stop)
			break;

		work = thread_pool_work_get(pool); // get next idle thread
		pool->working_count++;
		pthread_mutex_unlock(&(pool->work_mutex)); // release lock

		if (work)
		{
			work->func(work->arg); // do work
			thread_pool_work_destroy(work); // destroy object
		}

		pthread_mutex_lock(&(pool->work_mutex)); // grab lock to decrease working_count
		pool->working_count--;

		// If there are no threads working and there are no items in the queue, send a signal to inform the wait function to wake up
		if (!pool->stop && !pool->working_count && !pool->work_first)
			pthread_cond_signal(&(pool->working_cond));
		pthread_mutex_unlock(&(pool->work_mutex)); // release lock
	}

	pool->thread_count--;
	pthread_cond_signal(&(pool->working_cond));
	pthread_mutex_unlock(&(pool->work_mutex)); // release lock

	return NULL;
}

thread_pool_t* thread_pool_create(size_t size)
{
	thread_pool_t* pool;
	pthread_t thread;
	if (!size)
		size = 2;

	pool = calloc(1, sizeof(*pool));
	pool->thread_count = size;

	pthread_mutex_init(&(pool->work_mutex), NULL);
	pthread_cond_init(&(pool->work_cond), NULL);
	pthread_cond_init(&(pool->working_cond), NULL);

	pool->work_first = pool->work_last = NULL;

	for (int i = 0; i < size; i++) // create threads
	{
		pthread_create(&thread, NULL, thread_pool_worker, pool);
		pthread_detach(thread);
	}

	return pool;
}

void thread_pool_destroy(thread_pool_t* pool)
{
	thread_pool_work_t* current;
	thread_pool_work_t* next;

	if (!pool) // sanity check
		return;


	pthread_mutex_lock(&(pool->work_mutex));	// grab lock

	current = pool->work_first;
	while (current)								// destroy all the work objects in the queue
	{
		next = current->next;
		thread_pool_work_destroy(current);
		current = next;
	}

	pool->stop = true;							// stop all work that is currently being made
	pthread_cond_broadcast(&(pool->work_cond));	// unblock the threads that are currently blocked
	pthread_mutex_unlock(&(pool->work_mutex));	// release lock

	thread_pool_wait(pool);

	// cleanup
	pthread_mutex_destroy(&(pool->work_mutex));
	pthread_cond_destroy(&(pool->work_cond));
	pthread_cond_destroy(&(pool->working_cond));

	free(pool);
}

bool thread_pool_add_work(thread_pool_t* pool, thread_func_t func, void* arg)
{
	thread_pool_work_t* work = NULL;

	if (!pool) // sanity check
		return NULL;

	if (!(work = thread_pool_work_create(func, arg)))	// assign work to a new object and check if it actually was made
		return false;

	pthread_mutex_lock(&(pool->work_mutex));			// grab lock

	if (!pool->work_first)								// queue is empty, link work to be both first and last
	{
		pool->work_first = work;
		pool->work_last = pool->work_first;
	}
	else												// queue was not empty
	{
		pool->work_last->next = work;					// make it reachable from the current last object
		pool->work_last = work;							// link work to be last in queue
	}

	pthread_cond_broadcast(&(pool->work_cond));			// unblock the threads that are currently blocked
	pthread_mutex_unlock(&(pool->work_mutex));			// release lock

	return true;
}

void thread_pool_wait(thread_pool_t* pool)
{
	if (!pool) // sanity check
		return;


	pthread_mutex_lock(&(pool->work_mutex));
	while (true)
	{
		// wait while there are threads working OR the threads are stopping and not all have exited yet
		if ((!pool->stop && pool->working_count) || (pool->stop && pool->thread_count))
			pthread_cond_wait(&(pool->working_cond), &(pool->work_mutex));
		else
			break;
	}
	pthread_mutex_unlock(&(pool->work_mutex));
}

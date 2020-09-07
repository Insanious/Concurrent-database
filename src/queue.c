#include "queue.h"

queue_t* new_queue(size_t size)
{
	queue_t* queue = (queue_t*)malloc(sizeof(queue_t));
	queue->requests = (request_t**)malloc(size * sizeof(request_t*));
	queue->max_size = size;
	queue->size = queue->front = 0;
	queue->back = -1;
	// pthread_mutex_init(&(queue->lock), NULL);

	return queue;
}

void delete_queue(queue_t* queue)
{
	if (!queue)
		return;

	free(queue->requests);
	free(queue);
}

bool enqueue(queue_t* queue, request_t* req)
{
	if (queue->size == queue->max_size)
		return false;

	queue->back = (queue->back + 1) % queue->max_size;
	queue->requests[queue->back] = req;
	queue->size++;

	return true;
}

request_t* front(queue_t* queue)
{
	return (empty(queue)) ? NULL : queue->requests[queue->front];
}

request_t* dequeue(queue_t* queue)
{
	if (!queue->size)
		return NULL;

	request_t* front = queue->requests[queue->front];	// save return value
	queue->requests[queue->front] = NULL;				// remove from queue

	queue->front = (queue->front + 1) % queue->max_size;
	queue->size--;

	return front;
}

bool empty(queue_t* queue)
{
	return queue->size == 0;
}

size_t size(queue_t* queue)
{
	return queue->size;
}

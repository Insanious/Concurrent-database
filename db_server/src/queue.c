#include "queue.h"

queue_t* new_queue(size_t size)
{
	queue_t* queue = (queue_t*)malloc(sizeof(queue_t));
	queue->requests = (client_request**)malloc(size * sizeof(client_request*));
	queue->max_size = size;
	queue->size = queue->front = 0;
	queue->back = -1;

	return queue;
}

void delete_queue(queue_t* queue)
{
	if (!queue)
		return;

	free(queue->requests);
	free(queue);
}

bool enqueue(queue_t* queue, client_request* req)
{
	if (queue->size == queue->max_size)
		return false;

	queue->back = (queue->back + 1) % queue->max_size;
	queue->requests[queue->back] = req;
	queue->size++;

	return true;
}

client_request* dequeue(queue_t* queue)
{
	if (!queue->size)
		return NULL;

	client_request* front = queue->requests[queue->front];	// save return value
	queue->requests[queue->front] = NULL;				// remove from queue

	queue->front = (queue->front + 1) % queue->max_size;
	queue->size--;

	return front;
}

client_request* front(queue_t* queue)
{
	return (empty(queue)) ? NULL : queue->requests[queue->front];
}

bool empty(queue_t* queue)
{
	return queue->size == 0;
}

size_t size(queue_t* queue)
{
	return queue->size;
}

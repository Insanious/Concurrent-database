#ifndef QUEUE_H
#define QUEUE_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <pthread.h>
#include "request.h"

typedef struct queue_t queue_t;

struct queue_t
{
	request_t** requests;
	// pthread_mutex_t lock;
	size_t size;
	size_t max_size;
	size_t front;
	size_t back;
};

queue_t* new_queue(size_t size);
void delete_queue(queue_t* queue);
bool enqueue(queue_t* queue, request_t* req);
request_t* dequeue(queue_t* queue);
request_t* front(queue_t* queue);
bool empty(queue_t* queue);
size_t size(queue_t* queue);

#endif

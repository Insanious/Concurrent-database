#ifndef SERVER_H
#define SERVER_H

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <pthread.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>

#include "request.h"
#include "queue.h"
#include "thread_pool.h"
#include "db_functions.h"

typedef struct server server_t;

struct server
{
	queue_t* request_queue;
	size_t queue_size;

	thread_pool_t* pool;
	pthread_mutex_t enqueue_lock;
	sem_t empty_sem;
	sem_t full_sem;

	size_t socket;
	struct sockaddr_in address;
	struct sockaddr_storage storage;
	socklen_t address_size;

	bool quit;
};


server_t* server_create(size_t queue_size, size_t nr_of_threads);
void server_listen(server_t* server);
void server_destroy(server_t* server);

// void handle_connection1(void* arg);


#endif

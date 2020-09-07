#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h> // for open
#include <unistd.h> // for close
#include <pthread.h>
#include <semaphore.h>

#include "request.h"
#include "db_functions.h"
#include "queue.h"
#include "thread_pool.h"
#include "server.h"


char client_message[2000];
pthread_mutex_t enqueue_lock;
sem_t empty_sem;
sem_t full_sem;

queue_t* request_queue = NULL;
thread_pool_t* pool = NULL;


void handle_connection(void* arg)
{
	int new_socket = *((int *)arg);
	recv(new_socket, client_message, 2000, 0);
	close(new_socket);

	request_t* req = parse_request(client_message);
	printf("new incoming connection...\n");

	sem_wait(&empty_sem); // wait here until the queue is not full
	pthread_mutex_lock(&enqueue_lock);
	// the loop here is actually unnessecary since the thread got past the empty semaphore but just for sanity we put it in a while loop
	while (!enqueue(request_queue, req)); // try to enqueue until it works
	pthread_mutex_unlock(&enqueue_lock);
	sem_post(&full_sem); // signal that the queue is not empty anymore
}


void assign_work(void* arg)
{
	request_t* req = NULL;
	while (true)
	{
		sem_wait(&full_sem); // wait here until the queue is not empty
		// the check here is actually unnessecary since the thread got past the full semaphore but just for sanity we put it in an if statement
		if ((req = dequeue(request_queue))) // check if dequeue worked
		{
			thread_pool_add_work(pool, execute_request, req);
			sem_post(&empty_sem); // signal that the queue is not full anymore
		}
	}
}


int main(int argc, char *argv[])
{
	server_t* server = server_create(0, 0);
	size_t queue_size = 10;
	request_queue = new_queue(10);
	pool = thread_pool_create(10);

	int server_socket, new_socket;
	struct sockaddr_in server_address;
	struct sockaddr_storage server_storage;
	socklen_t address_size;

	pthread_mutex_init(&enqueue_lock, NULL);
	sem_init(&empty_sem, 0, queue_size); // start size is queue size since there are queue_size empty slots
	sem_init(&full_sem, 0, 0); // start size is 0 since the queue is empty
	thread_pool_add_work(pool, assign_work, NULL);
	// create socket
	server_socket = socket(PF_INET, SOCK_STREAM, 0);

	// Address Family = Internet
	server_address.sin_family = AF_INET;

	// set port number with proper byte order
	server_address.sin_port = htons(7798);

	// set ip address to localhost
	server_address.sin_addr.s_addr = inet_addr("192.168.0.2");

	// set all bits of the padding field to 0
	memset(server_address.sin_zero, '\0', sizeof server_address.sin_zero);

	// bind the address struct to the socket
	bind(server_socket, (struct sockaddr *) &server_address, sizeof(server_address));

	if (listen(server_socket, 20) == 0)
		printf("Listening...\n");

	while (true)
	{
		address_size = sizeof(server_storage);
		new_socket = accept(server_socket, (struct sockaddr *) &server_storage, &address_size); // wait until new connection
		thread_pool_add_work(pool, handle_connection, &new_socket);
	}

	// request_t* req = NULL;
	// while (!empty(request_queue))
	// {
	// 	req = dequeue(request_queue);
	// 	thread_pool_add_work(pool, execute_request, req);
	// }
	//
	// thread_pool_wait(pool);
	thread_pool_destroy(pool);

	// print_request(request);

	// destroy_request(req);

	delete_queue(request_queue);

	return 0;
}

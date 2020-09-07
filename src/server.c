#include "server.h"

char client_message[2000];

server_t* server_create(size_t queue_size, size_t nr_of_threads)
{
	server_t* server = NULL;

	if (queue_size <= 0)
		queue_size = 8;

	if (nr_of_threads <= 0)
		nr_of_threads = 8;

	server = calloc(1, sizeof(*server));

	server->pool = thread_pool_create(nr_of_threads);
	server->request_queue = new_queue(queue_size);

	pthread_mutex_init(&server->enqueue_lock, NULL);
	sem_init(&server->empty_sem, 0, queue_size);		// start size is queue size since there are queue_size empty slots
	sem_init(&server->full_sem, 0, 0);					// start size is 0 since the queue is empty

	server->socket = socket(PF_INET, SOCK_STREAM, 0);	// create socket
	server->address.sin_family = AF_INET;				// Address Family = Internet
	server->address.sin_port = htons(7798);				// set port number with proper byte order
	server->address.sin_addr.s_addr = inet_addr("192.168.0.2");							// set ip address to localhost
	memset(server->address.sin_zero, '\0', sizeof(server->address.sin_zero));				// set all bits of the padding field to 0
	bind(server->socket, (struct sockaddr *) &(server->address), sizeof(server->address));	// bind the address struct to the socket

	server->quit = false;

	return server;
}

void server_listen(server_t* server)
{
	if (listen(server->socket, 20) != 0)
		printf("error: listen failed\n");

	printf("Listening...\n");

	size_t new_socket;
	while (true)
	{
		server->address_size = sizeof(server->storage);
		new_socket = accept(server->socket, (struct sockaddr *) &server->storage, &server->address_size); // wait until new connection
		// thread_pool_add_work(server->pool, handle_connection1, &new_socket);
	}
}

// TODO: find a way to get the server object into this function
// void handle_connection1(void* arg)
// {
// 	size_t new_socket = *((size_t *)arg);
// 	recv(new_socket, client_message, 2000, 0);
// 	close(new_socket);
//
// 	request_t* req = parse_request(client_message);
// 	printf("new incoming connection...\n");
//
// 	sem_wait(&(server->empty_sem)); // wait here until the queue is not full
// 	pthread_mutex_lock(&(server->enqueue_lock));
// 	// the loop here is actually unnessecary since the thread got past the empty semaphore but just for sanity we put it in a while loop
// 	while (!enqueue(server->request_queue, req)); // try to enqueue until it works
// 	pthread_mutex_unlock(&(server->enqueue_lock));
// 	sem_post(&(server->full_sem)); // signal that the queue is not empty anymore
// }

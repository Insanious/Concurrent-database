#include "server.h"

void handle_connection(void* arg)
{
	connection_args* args = ((connection_args *)arg);
	request_t* req = parse_request(args->msg);
	sem_wait(&(args->server->empty_sem)); // wait here until the queue is not full
	pthread_mutex_lock(&(args->server->enqueue_lock)); // lock so other threads can't enqueue
	// the loop here is actually unnessecary since the thread got past the empty semaphore but just for sanity we put it in a while loop
	while (!enqueue(args->server->request_queue, req)); // try to enqueue until it works
	pthread_mutex_unlock(&(args->server->enqueue_lock)); // unlock so other threads can enqueue
	sem_post(&(args->server->full_sem)); // signal that the queue is not empty anymore

	// cleanup
	free(args->msg);
	free(args);
}

void assign_work(void* arg)
{
	server_t* server = (server_t *)arg;

	request_t* req = NULL;
	while (true)
	{
		sem_wait(&(server->full_sem)); // wait here until the queue is not empty
		// the check here is actually unnessecary since the thread got past the full semaphore but just for sanity we put it in an if statement
		if ((req = dequeue(server->request_queue))) // check if dequeue worked
		{
			thread_pool_add_work(server->pool, execute_request, req);
			sem_post(&(server->empty_sem)); // signal that the queue is not full anymore
		}
	}
}


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
	sem_init(&(server->empty_sem), 0, queue_size);		// start size is queue size since there are queue_size empty slots
	sem_init(&(server->full_sem), 0, 0);					// start size is 0 since the queue is empty

	server->socket = socket(PF_INET, SOCK_STREAM, 0);	// create socket
	if (setsockopt(server->socket, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0)
		perror("setsockopt");
	server->address.sin_family = AF_INET;				// Address Family = Internet
	server->address.sin_port = htons(7798);				// set port number with proper byte order
	server->address.sin_addr.s_addr = inet_addr("192.168.0.2");							// set ip address to localhost
	memset(server->address.sin_zero, '\0', sizeof(server->address.sin_zero));				// set all bits of the padding field to 0
	bind(server->socket, (struct sockaddr *) &(server->address), sizeof(server->address));	// bind the address struct to the socket

	server->quit = false;

	return server;
}

void server_listen(server_t* server, size_t nr_of_sockets)
{
	if (listen(server->socket, 30) != 0)
		perror("error: listen failed\n");

	printf("Listening...\n");
	fflush(stdout);

	size_t new_socket;
	size_t len = 0;
	char client_message[2000];

	while (true)
	{
		server->address_size = sizeof(server->storage);
		if ((new_socket = accept(server->socket, (struct sockaddr *) &server->storage, &server->address_size)) == -1) // wait until new connection
		{
			perror("accept");
			continue;
		}

		memset(&client_message, 0, 2000); // clear

		if (recv(new_socket, client_message, 2000, 0) == -1)
		{
			perror("recv");
			continue;
		}

		if (close(new_socket) == -1)
			fprintf(stdout, "\n%s\tclose(socket(%ld))", strerror(errno), new_socket);

		connection_args* args = malloc(sizeof(connection_args)); // malloc new args so it can persist with the new thread
		args->server = server;

		len = strlen(client_message);
		char *msg = (char*)malloc(len * sizeof(char)); // malloc new msg so it can persist with the new thread
		strcpy(msg, client_message); // copy client message to a new string
		args->msg = msg;

		thread_pool_add_work(server->pool, handle_connection, args);
	}
}

void server_destroy(server_t* server)
{
	if (!server) // sanity check
		return;

	thread_pool_wait(server->pool);
	thread_pool_destroy(server->pool);
	delete_queue(server->request_queue);

	free(server);
}

void server_init(server_t* server)
{
	thread_pool_add_work(server->pool, assign_work, server);
}

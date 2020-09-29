#include "server.h"

static bool client_newline(char **msg) {
    return (strlen(*msg) == 2 && (int)(*msg)[0] == 13 && (int)(*msg)[1] == 10);
}

void handle_connection(void *arg) {
    connection_args *args = ((connection_args *)arg);

    client_request *cli_req = (client_request *)malloc(sizeof(client_request));
    request_t *req = parse_request(args->msg, &cli_req->error);

    cli_req->request = req;
    cli_req->client_socket = args->socket;
    cli_req->server = args->server;
    sem_wait(
        &(args->server->empty_sem)); // wait here until the queue is not full
    pthread_mutex_lock(
        &(args->server->enqueue_lock)); // lock so other threads can't enqueue
    // the loop here is actually unnessecary since the thread got past the
    // empty_sem but just for sanity we put it in a while loop
    while (!enqueue(args->server->request_queue, cli_req))
	; // try to enqueue until it works
    pthread_mutex_unlock(
        &(args->server->enqueue_lock)); // unlock so other threads can enqueue
    sem_post(&(
        args->server->full_sem)); // signal that the queue is not empty anymore

    // cleanup
    free(args->msg);
    free(args);
}

void assign_work(void *arg) {
    server_t *server = (server_t *)arg;
    if (!server) {
	perror("assign_work arg");
	return;
    }

    client_request *cli_req = NULL;
    while (true) {
	sem_wait(&(server->full_sem)); // wait here until the queue is not empty
	// the check here is actually unnessecary since the thread got past the
	// full_sem but just for sanity we put it in an if statement
	if ((cli_req =
	         dequeue(server->request_queue))) // check if dequeue worked
	{
	    thread_pool_add_work(server->pool, execute_request, cli_req);
	    sem_post(&(server->empty_sem)); // signal that the queue is not full
	                                    // anymore
	}
    }
}

// server_t *server_create(size_t queue_size, size_t nr_of_threads)
server_t *server_create(bool daemon, size_t port, size_t request_handling,
                        char *logfile) {
    size_t queue_size = 32;
    size_t nr_of_threads = 8;

    server_t *server = NULL;

    server = calloc(1, sizeof(*server));

    server->pool = thread_pool_create(nr_of_threads);
    server->request_queue = new_queue(queue_size);
    server->queue_size = queue_size;
    server->logfile = logfile;

    pthread_mutex_init(&server->enqueue_lock, NULL);
    sem_init(&(server->empty_sem), 0,
             queue_size); // start size is queue size since there are queue_size
                          // empty slots
    sem_init(&(server->full_sem), 0,
             0); // start size is 0 since the queue is empty

    server->socket = socket(PF_INET, SOCK_STREAM, 0); // create socket
    if (setsockopt(server->socket, SOL_SOCKET, SO_REUSEADDR, &(int){1},
                   sizeof(int)) < 0) // reuse port
	perror("setsockopt");
    server->address.sin_family = AF_INET; // Address Family = Internet
    server->port = port;
    server->address.sin_port =
        htons(port); // set port number with proper byte order
    server->address.sin_addr.s_addr =
        inet_addr(IP_ADDR); // set ip address to localhost
    memset(server->address.sin_zero, '\0',
           sizeof(server->address
                      .sin_zero)); // set all bits of the padding field to 0
    bind(server->socket, (struct sockaddr *)&(server->address),
         sizeof(server->address)); // bind the address struct to the socket

    return server;
}

void server_listen(server_t *server) {
    if (listen(server->socket, 30) != 0)
	perror("error: listen failed\n");

    printf("Listening on port %ld...\n", server->port);
    fflush(stdout);

    size_t new_socket;
    size_t length = 0;
    char client_msg[1024];
    char *msg = NULL;

    size_t max_socket = server->socket;
    fd_set ready_sockets;
    FD_ZERO(&(server->current_sockets));
    FD_SET(server->socket,
           &(server->current_sockets)); // add server sockets to fd set

    while (true) {
	ready_sockets =
	    server->current_sockets; // copy current sockets to new fd_set since
	                             // select is destructive

	if (select(FD_SETSIZE, &ready_sockets, NULL, NULL, NULL) <
	    0) // check socket descriptors
	    perror("select");

	for (size_t i = 0; i <= max_socket; i++) {
	    if (!FD_ISSET(
	            i, &ready_sockets)) // nothing to read on socket descriptor
		continue;

	    new_socket = i;
	    if (new_socket == server->socket) // new connection
	    {
		server->address_size = sizeof(server->storage);
		if ((new_socket = accept(server->socket,
		                         (struct sockaddr *)&server->storage,
		                         &server->address_size)) == -1) {
		    perror("accept");
		    continue;
		}
		// add new connection to socket descriptors
		FD_SET(new_socket, &(server->current_sockets));
		max_socket = new_socket;
		continue;
	    }

	    memset(&client_msg, 0, 1024); // clear memory
	    if (recv(new_socket, client_msg, 1024, 0) ==
	        -1) // receive data from socket
	    {
		perror("recv");
		continue;
	    }

	    if (!(length = strlen(client_msg))) // nothing was received
		continue;
	    msg = (char *)malloc((length + 1) *
	                         sizeof(char)); // malloc new msg so it can
	                                        // persist with the new thread
	    strcpy(msg, client_msg); // copy client message to a new string

	    if (client_newline(
	            &msg)) // the client only sent newline and no request
	    {
		free(msg);
		continue;
	    }

	    // malloc new args so it can persist through the new thread
	    connection_args *args = malloc(sizeof(connection_args));
	    args->server = server;
	    args->socket = new_socket;
	    args->msg = msg;

	    thread_pool_add_work(server->pool, handle_connection, args);
	}
    }
}

void server_destroy(server_t *server) {
    if (!server) // sanity check
	return;

    thread_pool_wait(server->pool);
    thread_pool_destroy(server->pool);
    delete_queue(server->request_queue);

    free(server);
}

void server_init(server_t *server) {
    thread_pool_add_work(server->pool, assign_work, server);
}

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


int main(int argc, char *argv[])
{
	server_t* server = server_create(0, 0);
	server_init(server);
	server_listen(server);

	return 0;
}

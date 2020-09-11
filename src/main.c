#include "server.h"

int main(int argc, char *argv[])
{
	server_t* server = server_create(0, 0);
	server_init(server);
	server_listen(server);

	return 0;
}

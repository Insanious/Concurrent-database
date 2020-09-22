#include "server.h"

int main(int argc, char *argv[])
{
	server_t* server = server_create(8, 8);
	if (!server)
	{
		perror("server_create");
		return 1;
	}
	server_init(server);
	server_listen(server);

	return 0;
}

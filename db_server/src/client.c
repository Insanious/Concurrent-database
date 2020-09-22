#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <fcntl.h>	// for open
#include <unistd.h> // for close
#include <pthread.h>
#include "private_variables.h"

int main(int argc, char *argv[])
{
	if (argc != 2)
	{
		printf("Provide one request string in quotation marks!\n");
		exit(1);
	}

	char message[1024];
	int client_socket;
	struct sockaddr_in server_address;
	socklen_t address_size;

	// Create the socket.
	client_socket = socket(PF_INET, SOCK_STREAM, 0);
	if (setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0)
		printf("setsockopt(SO_REUSEADDR) failed\n");

	//Configure settings of the server address
	// Address family is Internet
	server_address.sin_family = AF_INET;

	//Set port number, using htons function
	server_address.sin_port = htons(7798);

	//Set IP address to localhost
	server_address.sin_addr.s_addr = inet_addr(IP_ADDR);
	memset(server_address.sin_zero, '\0', sizeof(server_address.sin_zero));

	//Connect the socket to the server using the address
	address_size = sizeof server_address;
	connect(client_socket, (struct sockaddr *)&server_address, address_size);
	// while ((connect(client_socket, (struct sockaddr *) &server_address, address_size) == -1));
	strcpy(message, argv[1]);

	if (send(client_socket, message, strlen(message), 0) < 0)
		printf("Send failed\n");
	// else
	// 	printf("sent\n");

	memset(&message, 0, 1024); // clear

	if (recv(client_socket, message, 1024, 0) == -1)
	{
		perror("recv");
		return 1;
	}
	printf("%s\n", message);

	close(client_socket);

	return 0;
}

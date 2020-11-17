#ifndef SERVER_H
#define SERVER_H

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define IP_ADDR "127.0.0.1"

#include "db_functions.h"
#include "queue.h"
#include "request.h"
#include "thread_pool.h"

// #define HELP "help me i suck at dis"
#define HELP "-h\t\tPrint this text.\n-p <port>\tListen to port number port.\n-d\t\tRun as a daemon instead of as a normal program.\n-l <logfile>\tLog to logfile. If this option is not specified,\n\t\tlogging will be output to syslog, which is the default.\n-s [prefork]"

#define THREAD 0
#define PREFORK 1
#define FORK 2
#define MUX 3

typedef struct server server_t;
struct server {
    queue_t *request_queue;
    size_t queue_size;
    char *log_file;

    thread_pool_t *pool;
    pthread_mutex_t enqueue_lock;
    sem_t empty_sem;
    sem_t full_sem;

    size_t socket;
    size_t port;
    struct sockaddr_in address;
    struct sockaddr_storage storage;
    socklen_t address_size;
    fd_set current_sockets;
};

typedef struct connection_args connection_args;
struct connection_args {
    server_t *server;
    size_t socket;
    char *msg;
};

void assign_work(void *arg);

server_t *server_create(bool daemon, size_t port, size_t request_handling, char *log_file);
void server_listen(server_t *server);
void server_destroy(server_t *server);
void server_init(server_t *server);

void daemonize_server(char *log_file);
char *get_ip_from_socket_fd(int fd);

#endif

#include "server.h"

int main(int argc, char *argv[]) {
    bool daemon = false;
    size_t port = 7798;
    size_t request_handling = 1;
    char *logfile = NULL;
    char *second_arg = NULL;

    // start at one because the first argument is the name of the executable
    for (size_t i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0)
            printf("%s\n", HELP);
        else if (strcmp(argv[i], "-d") == 0)
            daemon = true;
        else {
            if (i + 1 >= argc) {
                printf("error: expected a positional argument\n");
                exit(EXIT_FAILURE);
            }
            second_arg = argv[i + 1];

            if (strcmp(argv[i], "-p") == 0) {
                if ((port = strtoumax(second_arg, NULL, 10)) == 0) {
                    printf("error: expected an integer port number\n");
                    exit(EXIT_FAILURE);
                } else if (port > 0xFFFF || port < 1024) {
                    printf("error: expected a valid port number in the range (1024-65535) but got %s\n", second_arg);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(argv[i], "-l") == 0) {
                logfile = second_arg;
            } else if (strcmp(argv[i], "-s") == 0) {
                if (strcmp(second_arg, "fork") == 0)
                    request_handling = FORK;
                else if (strcmp(second_arg, "thread") == 0)
                    request_handling = THREAD;
                else if (strcmp(second_arg, "prefork") == 0)
                    request_handling = PREFORK;
                else if (strcmp(second_arg, "mux") == 0)
                    request_handling = MUX;
                else {
                    printf("error: expected a one of [fork, thread, prefork, mux] but got %s\n", second_arg);
                    exit(EXIT_FAILURE);
                }
            }

            i++;
        }
    }
    // we have currently only implemented the 'prefork' option
    if (request_handling != PREFORK) {
        printf("%s\n", HELP);
        exit(3);
    }

    server_t *server = server_create(daemon, port, request_handling, logfile);
    if (!server) {
        perror("server_create");
        return 1;
    }
    server_init(server);
    server_listen(server);

    return 0;
}

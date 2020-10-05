#ifndef DB_FUNCTIONS_H
#define DB_FUNCTIONS_H

#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE
#define _GNU_SOURCE
#include <errno.h>

#include <fcntl.h>
#include <math.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <syslog.h>
#include <unistd.h>

#include "dynamic_string.h"
#include "queue.h"
#include "request.h"
#include "server.h"
#include "table_t.h"

#define META_FILE "../database/meta.txt"
#define DATA_FILE_PATH "../database/"
#define DATA_FILE_ENDING ".txt"
#define COL_DELIM ","
#define TYPE_DELIM " "
#define ROW_DELIM "\n"
#define START_LENGTH 64
#define MULTIPLIER 2
#define CHARS_PER_INT 10
#define CHARS_PER_SEND 400
#define PADDING '0'

typedef struct return_value return_value;
struct return_value {
    char *msg;
    bool success;
};

void execute_request(void *arg);

void create_table(client_request *cli_req, return_value *ret_val);
void print_tables(return_value *ret_val);
void print_schema(char *name, return_value *ret_val);
void add_table(table_t *table, FILE *meta, char *log_file);
void select_table(char *name, client_request *cli_req);
void drop_table(client_request *cli_req /*char *name*/, return_value *ret_val);
bool table_exists(char *name, FILE *meta);
int create_data_file(char *name);
void insert_data(request_t *req, return_value *ret_val);
void update_data(request_t *req, return_value *ret_val);
void create_template_column(char *name, FILE *meta, column_t **first, int *chars_in_row);
int create_full_data_path_from_name(char *name, char **full_path);
void log_to_file(char *file_name, const char *format, ...);

bool is_valid_varchar(column_t *col);

int column_to_buffer(column_t *table_column, column_t *input_column,
                     dynamicstr *output_buffer, char **ret_msg);
int populate_column(column_t *current, char *table_row);
int unpopulate_column(column_t *current);

#endif

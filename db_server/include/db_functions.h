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

extern char *log_file;

typedef struct is_primary_key is_primary_key;
struct is_primary_key {
	int total_row_size;
	int size_to_pk;
	char *name;
	bool found;
};

typedef struct table_column_t table_column_t;
struct table_column_t {
	/* name of the column */
	char *name;
	/* indicates the columns data type */
	char data_type;
	/* indicates if column is the PRIMARY KEY column */
	char is_primary_key;
	/* INT value for INSERT or UPDATE statement */
	char *char_val;
	/* Offset from the start of the row to this column*/
	int offset;
	/* total size of this column */
	int total_size;
	/* pointer to next column entry */
	table_column_t *next;
};

void execute_request(void *arg);

void create_table(client_request *cli_req, char **client_msg);
void print_tables(char **client_msg);
void print_schema(char *name, char **client_msg);
int add_table(table_t *table, dynamicstr *output_buffer, FILE *meta, char **error_msg);
void select_table(client_request *cli_req, char **client_msg);
void drop_table(client_request *cli_req, char **client_msg);
bool table_exists(char *name, FILE *meta);
void quit_connection(client_request *cli_req);
int create_data_file(char *name);
void insert_data(client_request *cli_req, char **client_msg);
void create_template_column(char *name, FILE *meta, table_column_t **first, int *chars_in_row);
int create_full_data_path_from_name(char *name, char **full_path);
void log_to_file(const char *format, ...);
void update_row(client_request *cli_req, char **client_msg);

bool is_valid_varchar(column_t *col);

int column_to_buffer(table_column_t *table_column, column_t *input_column,
					 dynamicstr *output_buffer, int primary_key, char **client_msg);
int populate_column(table_column_t *current, char *table_row, is_primary_key *is_pk);
int unpopulate_column(table_column_t *current);

#endif

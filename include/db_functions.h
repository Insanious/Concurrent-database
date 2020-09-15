#ifndef DB_FUNCTIONS_H
#define DB_FUNCTIONS_H

#define  _POSIX_C_SOURCE 200809L

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/socket.h>

#include "table_t.h"
#include "request.h"
#include "queue.h"

#define META_FILE "meta.txt"
#define COL_DELIM ","
#define TYPE_DELIM " "
#define ROW_DELIM "\n"
#define MULTIPLIER 2

typedef struct return_value return_value;
struct return_value
{
	char* msg;
	bool success;
};

void execute_request(void* arg);

void create_table(request_t* req, return_value* ret_val);
void print_tables(return_value* ret_val);
void print_schema(char* name, return_value* ret_val);
void add_table(table_t* table);
bool table_exists(char* name);

bool is_valid_varchar(column_t* col);

#endif

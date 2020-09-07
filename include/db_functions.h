#ifndef DB_FUNCTIONS_H
#define DB_FUNCTIONS_H

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "table_t.h"
#include "request.h"

#define META_FILE "meta.txt"
#define COL_DELIM ","
#define TYPE_DELIM " "
#define ROW_DELIM "\n"

/* Requests */
void execute_request(void* arg);

/* Tables */
bool create_table(request_t* req);
bool print_tables();
bool print_schema(char* name);
void add_table(table_t* table);
bool table_exists(char* name);

/* Columns */
bool is_valid_varchar(column_t* col);

#endif

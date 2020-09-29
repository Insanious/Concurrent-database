#ifndef REQUEST_H
#define REQUEST_H
#pragma GCC visibility push(default)

#define RT_CREATE   0
#define RT_TABLES   1
#define RT_SCHEMA   2
#define RT_DROP     3
#define RT_INSERT   4
#define RT_SELECT   5
#define RT_QUIT     6
#define RT_DELETE   7
#define RT_UPDATE   8

#define DT_INT      0
#define DT_VARCHAR  1

typedef struct request_t request_t;
typedef struct column_t column_t;

struct column_t {
    /* name of the column */
    char* name;
    /* indicates the columns data type */
    char data_type;
    /* indicates if column is the PRIMARY KEY column */
    char is_primary_key;
    /* INT value for INSERT or UPDATE statement */
    int int_val;
    /* string length for VARCHAR definition */
    int char_size;
    /* VARCHAR value for INSERT or UPDATE statement */
    char* char_val;
    /* pointer to next column entry */
    column_t* next;
};

struct request_t {
    /* type of the request */
    char request_type;
    /* name of the table this request is for */
    char* table_name;
    /* columns for which this request is for */
    column_t* columns;
    /* column which to use in the WHERE statement */
    column_t* where;
};

/*
 * Function: parse_request
 * -----------------------
 * parses database server command into request_t struct
 *
 * request_string: string containing one database command
 * parse_error: pointer to character pointer for error reporting
 *
 * returns: request_t filled with the information contained in the
 *          parsed command string
 *
 * errors: returns NULL in case of error
 *         allocates memory for error message and makes parse_error pointer
 *         point to it -- NOTE: this message buffer needs to be freed by the
 *         caller
 * */
request_t* parse_request(char* request_string, char** parse_error);
/*
 * Function: print_request
 * -----------------------
 * prints the information contained in the provided request_t
 * */
void print_request(request_t* request);
/*
 * Function: destroy_request
 * -------------------------
 * frees all memory associated with the provided request_t
 * including the request_t itself
 * */
void destroy_request(request_t* request);

#pragma GCC visibility pop
#endif

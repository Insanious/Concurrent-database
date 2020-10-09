#ifndef TABLE_T
#define TABLE_T

#include "request.h"

typedef struct table_t table_t;
struct table_t {
	/* name of the table */
	char *name;
	/* the columns in the table */
	column_t *columns;
	column_t *where;
};

#endif

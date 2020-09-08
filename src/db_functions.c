#include "db_functions.h"

void execute_request(void* arg)
{
	request_t* req = arg;

	bool success = false;
	switch(req->request_type)
	{
		case RT_CREATE:	success = create_table(req); break;
		case RT_TABLES:	success = print_tables(); break;
		case RT_SCHEMA:	success = print_schema(req->table_name); break;
		case RT_DROP:	printf("RT_DROP\n");/*success = create_table(req);*/ break;
		case RT_INSERT:	printf("RT_INSERT\n");/*success = create_table(req);*/ break;
		case RT_SELECT:	printf("RT_SELECT\n");/*success = create_table(req);*/ break;
		case RT_QUIT:	printf("RT_QUIT\n");/*success = create_table(req);*/ break;
		case RT_DELETE:	printf("RT_DELETE\n");/*success = create_table(req);*/ break;
		case RT_UPDATE:	printf("RT_UPDATE\n");/*success = create_table(req);*/ break;
	}

	if (success)
		printf("request executed correctly\n");
	else
		printf("error: request did not execute correctly\n");

	destroy_request(req); // cleanup
}

bool create_table(request_t* req)
{
	table_t table;
	table.name = req->table_name;
	table.columns = req->columns;

	if (table_exists(table.name))
	{
		printf("error: table '%s' already exists\n", table.name);
		return false;
	}

	column_t* col = req->columns;
	while (col)
	{
		if (col->data_type == DT_VARCHAR && !is_valid_varchar(col))
		{
			printf("error: VARCHAR contained faulty value '%d'\n", col->char_size);
			return false;
		}

		col = col->next;
	}

	add_table(&table);

	return true;
}

bool print_tables()
{
	FILE* meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
		return true;

	// check database meta file for the table name
	char* token = NULL;
	char line[256];

	while (fgets(line, sizeof(line), meta))
	{
		token = strtok(line, COL_DELIM);
		printf("%s\n", token);
	}

	return true;
}

bool print_schema(char* name)
{
	FILE* meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		printf("%s does not exist\n", META_FILE);
		return false;
	}

	char* token = NULL;
	char line[256];
	while (fgets(line, sizeof(line), meta))
	{
		token = strtok(line, COL_DELIM);
		if (strcmp(token, name) != 0)
			continue;

		// found the table
		while ((token = strtok(0, TYPE_DELIM))) // print all the columns of the table
		{
			printf("%s\t", token); // print name
			if (strlen(token) < 8) // format output for smaller names
				printf("\t"); // print name

			token = strtok(0, COL_DELIM);
			printf("%s\n", token); // print type
		}

		return true;
	}

	printf("error: table '%s' does not exists\n", name);
	return false; // didn't find table, unsuccessful
}

void add_table(table_t* table)
{
	FILE* meta = fopen(META_FILE, "a");
	fprintf(meta, "%s%s", table->name, COL_DELIM);

	column_t* col = table->columns;
	while (col->next) // loop while the column is not the last one
	{
		// write each column to the file with the appropriate format
		if (col->data_type == DT_INT)	// INT name,
			fprintf(meta, "%s%sINT%s", col->name, TYPE_DELIM, COL_DELIM);
		else							// VARCHAR(N) name,
			fprintf(meta, "%s%sVARCHAR(%d)%s", col->name, TYPE_DELIM, col->char_size, COL_DELIM);

		col = col->next;
	}
	// handle last one separately to instead use a newline instead of the column delimiter
	if (col->data_type == DT_INT)	// INT name,
		fprintf(meta, "%s%sINT%s", col->name, TYPE_DELIM, ROW_DELIM);
	else							// VARCHAR(N) name,
		fprintf(meta, "%s%sVARCHAR(%d)%s", col->name, TYPE_DELIM, col->char_size, ROW_DELIM);

	fclose(meta);
}

bool table_exists(char* name)
{
	// check database meta file for the table name
	FILE* meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
		return false;

	char* token = NULL;
	char line[256];

	while (fgets(line, sizeof(line), meta)) // get each line of the meta file
	{
		token = strtok(line, COL_DELIM);
		if (strcmp(token, name) == 0)
			return true;
	}

	return false;
}

bool is_valid_varchar(column_t* col)
{
	return col->char_size >= 0;
}

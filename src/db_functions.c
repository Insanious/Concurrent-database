#include "db_functions.h"

static char *create_format_buffer(const char *format, ...)
{
	size_t len = 0;
	va_list args;
	char *buffer = NULL;
	len = vsnprintf(NULL, 0, format, args) + 1;
	buffer = (char *)malloc(len);
	va_start(args, format);
	vsnprintf(buffer, len, format, args);
	va_end(args);
	return buffer;
}

void execute_request(void *arg)
{
	client_request *cli_req = ((client_request *)arg);

	return_value ret_val;
	switch (cli_req->request->request_type)
	{
	case RT_CREATE:
		create_table(cli_req->request, &ret_val);
		break;
	case RT_TABLES:
		print_tables(&ret_val);
		break;
	case RT_SCHEMA:
		print_schema(cli_req->request->table_name, &ret_val);
		break;
	case RT_DROP:
		printf("RT_DROP\n"); /*ret_val = create_table(req);*/
		break;
	case RT_INSERT:
		printf("RT_INSERT\n"); /*ret_val = create_table(req);*/
		break;
	case RT_SELECT:
		printf("RT_SELECT\n"); /*ret_val = create_table(req);*/
		break;
	case RT_QUIT:
		printf("RT_QUIT\n"); /*ret_val = create_table(req);*/
		break;
	case RT_DELETE:
		printf("RT_DELETE\n"); /*ret_val = create_table(req);*/
		break;
	case RT_UPDATE:
		printf("RT_UPDATE\n"); /*success = create_table(req);*/
		break;
	}

	if (ret_val.msg && send(cli_req->client_socket, ret_val.msg, strlen(ret_val.msg), 0) < 0)
		perror("send\n");

	if (close(cli_req->client_socket) == -1)
		perror("close");

	// cleanup
	destroy_request(cli_req->request);
	free(cli_req);

	free(ret_val.msg);
}

void create_table(request_t *req, return_value *ret_val)
{
	table_t table;
	table.name = req->table_name;
	table.columns = req->columns;

	if (table_exists(table.name))
	{
		ret_val->msg = create_format_buffer("PrintFError: table '%s' already exists", table.name);
		ret_val->success = false;
		return;
	}

	column_t *col = req->columns;
	while (col)
	{
		if (col->data_type == DT_VARCHAR && !is_valid_varchar(col))
		{
			ret_val->msg = create_format_buffer("error: VARCHAR contained faulty value '%d'", col->char_size);
			ret_val->success = false;
			return;
		}

		col = col->next;
	}

	add_table(&table);

	ret_val->msg = create_format_buffer("successfully created table '%s'", table.name);
	ret_val->success = true;
}

void print_tables(return_value *ret_val)
{
	char *buffer;

	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		ret_val->msg = create_format_buffer("error: %s does not exist", META_FILE);
		ret_val->success = false;
		return;
	}

	buffer = (char *)malloc(2048 * sizeof(char)); // malloc and pray that 2048 is enough
	// check database meta file for the table name
	char *token = NULL;
	char line[256];
	for (int i = 0; i < 256; i++)
		line[i] = '\0';

	while (fgets(line, sizeof(line), meta))
	{
		token = strtok(line, COL_DELIM);
		strcat(buffer, token);
		strcat(buffer, "\n");
	}

	fclose(meta);

	ret_val->msg = buffer;
	ret_val->success = true;
}

void print_schema(char *name, return_value *ret_val)
{
	size_t len;
	char *buffer;

	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		ret_val->msg = create_format_buffer("error: %s does not exist", META_FILE);
		ret_val->success = false;
		return;
	}

	char *token = NULL;
	char line[256];
	for (int i = 0; i < 256; i++)
		line[i] = '\0';

	while (fgets(line, sizeof(line), meta))
	{
		token = strtok(line, COL_DELIM);
		if (strcmp(token, name) != 0)
			continue;

		// found the table
		char buf[2048];
		for (int i = 0; i < 2048; i++)
			buf[i] = '\0';

		while ((token = strtok(0, TYPE_DELIM))) // print all the columns of the table
		{
			strcat(buf, token);
			strcat(buf, "\t");
			if (strlen(token) < 8) // format output for smaller names
				strcat(buf, "\t");

			token = strtok(0, COL_DELIM);
			strcat(buf, token);
			strcat(buf, "\n");
		}

		len = strlen(buf);
		if (!(buffer = (char *)malloc(len + 1)))
			perror("malloc");
		strcpy(buffer, buf);
		ret_val->msg = buffer;
		ret_val->success = false;
		fclose(meta);
		return;
	}

	fclose(meta);

	ret_val->msg = create_format_buffer("error: table '%s' does not exists", name);
	ret_val->success = false;
}

void add_table(table_t *table)
{
	FILE *meta;
	if (!(meta = fopen(META_FILE, "a")))
	{
		perror("fopen");
		return;
	}
	fprintf(meta, "%s%s", table->name, COL_DELIM);

	column_t *col = table->columns;
	while (col->next) // loop while the column is not the last one
	{
		// write each column to the file with the appropriate format
		if (col->data_type == DT_INT) // INT name,
			fprintf(meta, "%s%sINT%s", col->name, TYPE_DELIM, COL_DELIM);
		else // VARCHAR(N) name,
			fprintf(meta, "%s%sVARCHAR(%d)%s", col->name, TYPE_DELIM, col->char_size, COL_DELIM);

		col = col->next;
	}
	// handle last one separately to instead use a newline instead of the column delimiter
	if (col->data_type == DT_INT) // INT name,
		fprintf(meta, "%s%sINT%s", col->name, TYPE_DELIM, ROW_DELIM);
	else // VARCHAR(N) name,
		fprintf(meta, "%s%sVARCHAR(%d)%s", col->name, TYPE_DELIM, col->char_size, ROW_DELIM);

	fclose(meta);
}

bool table_exists(char *name)
{
	// check database meta file for the table name
	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
		return false;

	char *token = NULL;
	char line[256];
	for (int i = 0; i < 256; i++)
		line[i] = '\0';

	while (fgets(line, sizeof(line), meta)) // get each line of the meta file
	{
		token = strtok(line, COL_DELIM);
		if (strcmp(token, name) == 0)
		{
			fclose(meta);
			return true;
		}
	}

	fclose(meta);

	return false;
}

bool is_valid_varchar(column_t *col)
{
	return col->char_size >= 0;
}
#include "db_functions.h"
#include "dynamic_string.h"

static size_t realloc_str(char **str, size_t size)
{
	if (!*(str))
		return 0;

	if (!(*str = (char *)realloc(*str, (size_t)(size * MULTIPLIER))))
		perror("realloc");

	return (size_t)(size * MULTIPLIER);
}

static char *create_format_buffer(const char *format, ...)
{
	if (!format)
		return NULL;

	va_list args;

	va_start(args, format);
	size_t length = vsnprintf(NULL, 0, format, args) + 1;
	va_end(args);

	char *buffer = (char *)malloc(length);
	va_start(args, format);
	vsnprintf(buffer, length, format, args);
	va_end(args);

	return buffer;
}

void execute_request(void *arg)
{
	client_request *cli_req = ((client_request *)arg);
	return_value ret_val;
	if (cli_req->error == NULL)
	{

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
	}
	else
	{
		if (send(cli_req->client_socket, cli_req->error, strlen(cli_req->error), 0) < 0)
			perror("send\n");

		free(cli_req->error);
	}
	if (close(cli_req->client_socket) == -1)
		perror("close");
	// cleanup
	destroy_request(cli_req->request);
	free(cli_req);
}

void create_table(request_t *req, return_value *ret_val)
{
	table_t table;
	table.name = req->table_name;
	table.columns = req->columns;

	FILE *meta = fopen(META_FILE, "r");
	int metaDescriptor = fileno(meta);
	struct flock lock;
	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_WRLCK;

	fcntl(metaDescriptor, F_SETLKW, &lock);
	if (table_exists(table.name, meta))
	{
		ret_val->msg = create_format_buffer("error: table '%s' already exists", table.name);
		ret_val->success = false;
		fclose(meta);
		return;
	}

	column_t *col = req->columns;
	while (col)
	{
		if (col->data_type == DT_VARCHAR && !is_valid_varchar(col))
		{
			ret_val->msg = create_format_buffer("error: VARCHAR contained faulty value '%d'", col->char_size);
			ret_val->success = false;
			fclose(meta);
			return;
		}

		col = col->next;
	}

	add_table(&table, meta);
	create_data_file(&table.name);

	fclose(meta);

	ret_val->msg = create_format_buffer("successfully created table '%s'", table.name);
	ret_val->success = true;
}

void print_tables(return_value *ret_val)
{
	char *buffer;
	ret_val->success = false;

	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		ret_val->msg = create_format_buffer("error: %s does not exist", META_FILE);
		return;
	}

	char *token = NULL;
	char *line = NULL;
	size_t nr_of_chars = 0;
	size_t buffer_length = START_LENGTH;
	buffer = (char *)calloc(buffer_length, sizeof(char));

	// check database meta file for the table name
	while (getline(&line, &nr_of_chars, meta) != -1)
	{
		token = strtok(line, COL_DELIM);
		// realloc if token can't fit in buffer
		while (strlen(token) > buffer_length - strlen(buffer))
			buffer_length = realloc_str(&buffer, buffer_length);
		strcat(buffer, token);
		strcat(buffer, "\n");
	}
	free(line);

	// remove last newline
	buffer[strlen(buffer) - 1] = '\0';

	fclose(meta);

	ret_val->msg = buffer;
	ret_val->success = true;
}

void print_schema(char *name, return_value *ret_val)
{
	ret_val->success = false;

	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		ret_val->msg = create_format_buffer("error: %s does not exist", META_FILE);
		return;
	}

	char *token = NULL;
	char *line = NULL;
	size_t nr_of_chars = 0;

	while (getline(&line, &nr_of_chars, meta) != -1)
	{
		token = strtok(line, COL_DELIM);
		if (strcmp(token, name) != 0)
			continue;

		// found the table
		size_t buffer_length = START_LENGTH;
		char *buffer = (char *)malloc(buffer_length * sizeof(char));

		while ((token = strtok(0, TYPE_DELIM))) // print all the columns of the table
		{
			while (strlen(token) + 2 > buffer_length - strlen(buffer)) // +2 for the tabs
				buffer_length = realloc_str(&buffer, buffer_length);

			strcat(buffer, token);
			strcat(buffer, "\t");
			if (strlen(token) < 8) // format output for smaller names
				strcat(buffer, "\t");

			token = strtok(0, COL_DELIM);
			while (strlen(token) + 1 > buffer_length - strlen(buffer)) // +1 for the newline
				buffer_length = realloc_str(&buffer, buffer_length);

			strcat(buffer, token);
			strcat(buffer, "\n");
		}

		ret_val->msg = buffer;
		ret_val->success = true;
		fclose(meta);
		return;
	}

	fclose(meta);

	ret_val->msg = create_format_buffer("error: table '%s' does not exists", name);
}

void add_table(table_t *table, FILE *meta)
{
	// Issue: When using bytelocking two tables of the same name could occur.
	// Solution: Lock the whole file, look for table name, if it doesn't exist add it, unlock the file.

	if (!(meta = freopen(NULL, "a", meta)))
	{
		perror("fopen");
		return;
	}

	int metaDescriptor = fileno(meta);

	struct flock lock;
	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_WRLCK;

	fcntl(metaDescriptor, F_SETLKW, &lock);

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
}

bool table_exists(char *name, FILE *meta)
{
	// check database meta file for the table name
	if (!(freopen(NULL, "r", meta))) // if the database is empty, the table can't exist in the database
		return false;

	char *token = NULL;
	char *line = NULL;
	size_t nr_of_chars = 0;

	while (getline(&line, &nr_of_chars, meta) != -1)
	{
		token = strtok(line, COL_DELIM);
		if (strcmp(token, name) == 0)
		{
			free(line);
			return true;
		}
	}
	free(line);

	return false;
}

bool is_valid_varchar(column_t *col)
{
	return col->char_size >= 0;
}

void create_data_file(char *t_name)
{
	int data_fd = open(t_name, O_CREAT);
	close(data_fd);
}
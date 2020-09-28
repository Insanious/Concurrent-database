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
	// cli_req->server = ((server_t *)cli_req->server);
	server_t *server = ((server_t *)cli_req->server);
	return_value ret_val;
	ret_val.success = false;
	ret_val.msg = NULL;

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
			insert_data(cli_req->request, &ret_val);
			break;
		case RT_SELECT:
			printf("RT_SELECT\n"); /*ret_val = create_table(req);*/
			break;
		case RT_QUIT:
			FD_CLR(cli_req->client_socket, &(server->current_sockets));	// clear socket descriptor from server
			// shutdown + close to ensure that both the socket and the telnet connection is closed
			if(shutdown(cli_req->client_socket, SHUT_RDWR) == -1)
				perror("shutdown");
			if(close(cli_req->client_socket) == -1)
				perror("close");
			break;
		case RT_DELETE:
			printf("RT_DELETE\n"); /*ret_val = create_table(req);*/
			break;
		case RT_UPDATE:
			printf("RT_UPDATE\n"); /*success = create_table(req);*/
			break;
		}
		if (ret_val.msg && send(cli_req->client_socket, ret_val.msg, strlen(ret_val.msg), 0) < 0)
			perror("send");
	}
	else
	{
		if (send(cli_req->client_socket, cli_req->error, strlen(cli_req->error), 0) < 0)
			perror("send\n");

		free(cli_req->error);
	}

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
		ret_val->msg = create_format_buffer("error: table '%s' already exists\n", table.name);
		fclose(meta);
		return;
	}

	column_t *col = req->columns;
	while (col)
	{
		if (col->data_type == DT_VARCHAR && !is_valid_varchar(col))
		{
			ret_val->msg = create_format_buffer("error: VARCHAR contained faulty value '%d'\n", col->char_size);
			fclose(meta);
			return;
		}

		col = col->next;
	}

	add_table(&table, meta);
	if (create_data_file(table.name) < 0)
	{
		ret_val->msg = create_format_buffer("error: could not create data file\n");
		fclose(meta);
		return;
	}

	fclose(meta);

	ret_val->msg = create_format_buffer("successfully created table '%s'\n", table.name);
	ret_val->success = true;
}

void print_tables(return_value *ret_val)
{
	char *buffer;

	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		ret_val->msg = create_format_buffer("error: %s does not exist\n", META_FILE);
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
	fclose(meta);

	ret_val->msg = buffer;
	ret_val->success = true;
}

void print_schema(char *name, return_value *ret_val)
{
	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		ret_val->msg = create_format_buffer("error: %s does not exist\n", META_FILE);
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

		// remove last newline
		buffer[strlen(buffer) - 1] = '\0';

		ret_val->msg = buffer;
		ret_val->success = true;
		fclose(meta);
		return;
	}

	fclose(meta);

	ret_val->msg = create_format_buffer("error: table '%s' does not exists\n", name);
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

int create_data_file(char *t_name)
{
	int name_size = strlen(t_name);
	char *final_name = (char *)malloc(strlen(DATA_FILE_PATH) + name_size + strlen(DATA_FILE_ENDING)+ 1);
	if (final_name == NULL)
		return -1;
	strcpy(final_name, DATA_FILE_PATH);
	strcat(final_name, t_name);
	strcat(final_name, DATA_FILE_ENDING);
	int data_fd = open(final_name, O_CREAT, 0644);
	close(data_fd);
	free(final_name);
	return 0;
}

void insert_data(request_t *req, return_value *ret_val)
{
	table_t table;
	table.name = req->table_name;
	table.columns = req->columns;

	int name_size = strlen(table.name);
	char *data_file_name = (char *)malloc(strlen(DATA_FILE_PATH) + name_size + 1);

	FILE *meta = fopen(META_FILE, "r");
	FILE *data_file = fopen(data_file_name, "a");

	int meta_descriptor = fileno(meta);
	int data_file_descriptor = fileno(data_file);

	struct flock meta_file_lock;
	struct flock data_file_lock;

	memset(&data_file_lock, 0, sizeof(data_file_lock));
	memset(&meta_file_lock, 0, sizeof(meta_file_lock));

	data_file_lock.l_type = F_WRLCK;
	meta_file_lock.l_type = F_WRLCK;

	fcntl(meta_descriptor, F_SETLKW, &meta_file_lock);
	fcntl(data_file_descriptor, F_SETLKW, &data_file_lock);

	// Get information from table, how many bytes is each column?
	// Make sure that excess space is filled with null characters
	// Check how INSERT fills up the request_t structure
	// Information about table can be a struct

	// getline into buffer, remove the table name and put it into populate column

	free(data_file_name);
	fclose(meta);
	fclose(data_file);
}

int populate_column(column_t *current, char *table_row)
{
  char column_name[50];
  char column_type[50];
  column_name[0] = '\0';
  column_type[0] = '\0';

  sscanf(table_row, "%s%*[ ]%[^,]", column_name, column_type);
  printf("Column: %s,%s\n", column_name, column_type);
  printf("Length: %ld, %ld\n", strlen(column_name), strlen(column_type));
  current->name = (char *)malloc(strlen(column_name) + 1);
  strcpy(current->name, column_name);

  if (column_type[0] == 'I')
  {
    current->data_type = DT_INT;
  }
  else
  {
    current->data_type = DT_VARCHAR;
    sscanf(column_type, "%*[^0123456789]%d", &current->char_size);
  }

  table_row = strtok(NULL, ",");
  if (table_row != NULL)
  {
    column_t *next = (column_t *)malloc(sizeof(column_t));
    next->next = NULL;
    populate_column(next, table_row);
    current->next = next;
  }

    return 0;
}

int unpopulate_column(column_t *current)
{
  free(current->name);
  current->name = NULL;
  if(current->next != NULL)
    unpopulate_column(current->next);
  free(current);
  current = NULL;
  return 0;
}

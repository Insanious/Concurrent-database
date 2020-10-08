#include "db_functions.h"

static size_t realloc_str(char **str, size_t size) {
	if (!*(str) || !(*str = (char *)realloc(*str, (size_t)(size * MULTIPLIER))))
		return 0;

	return (size_t)(size * MULTIPLIER);
}

static char *create_format_buffer(const char *format, ...) {
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

void execute_request(void *arg) {
	client_request *cli_req = ((client_request *)arg);
	// server_t *server = ((server_t *)cli_req->server);
	// return_value ret_val;
	// ret_val.msg = NULL;
	char *client_msg = NULL;

	if (cli_req->error) {
		if (send(cli_req->client_socket, cli_req->error, strlen(cli_req->error), 0) < 0)
			log_to_file("Error: Couldn't send() to socket %ld in execute_request()\n", cli_req->client_socket);

		free(cli_req->error);
		free(cli_req);
		return;
	}

	switch (cli_req->request->request_type) {
	case RT_CREATE:
		create_table(cli_req, &client_msg);
		break;
	case RT_TABLES:
		print_tables(&client_msg);
		break;
	case RT_SCHEMA:
		print_schema(cli_req->request->table_name, &client_msg);
		break;
	case RT_DROP:
		drop_table(cli_req, &client_msg);
		break;
	case RT_INSERT:
		insert_data(cli_req, &client_msg);
		break;
	case RT_SELECT:
		select_table(cli_req, &client_msg);
		break;
	case RT_QUIT:
		quit_connection(cli_req);
		break;
	case RT_DELETE:
		printf("RT_DELETE\n");
		break;
	case RT_UPDATE:
		printf("RT_UPDATE\n");
		break;
	}

	if (client_msg && send(cli_req->client_socket, client_msg, strlen(client_msg), 0) < 0) {
		log_to_file("Error: Couldn't send() to socket %ld in execute_request()\n", cli_req->client_socket);
		free(client_msg);
	}

	destroy_request(cli_req->request);
	free(cli_req);
}

void create_table(client_request *cli_req, char **client_msg) {
	table_t table;
	FILE *meta = NULL;
	table.name = cli_req->request->table_name;
	table.columns = cli_req->request->columns;

	// create file if it doesn't exists, and open it for reading
	meta = (access(META_FILE, F_OK) == -1) ? fopen(META_FILE, "w+") : fopen(META_FILE, "r");
	int metaDescriptor = fileno(meta);
	struct flock lock;
	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_WRLCK;

	fcntl(metaDescriptor, F_OFD_SETLKW, &lock);
	if (table_exists(table.name, meta)) {
		*client_msg = create_format_buffer("error: table '%s' already exists\n", table.name);
		fclose(meta);
		return;
	}

	column_t *col = cli_req->request->columns;
	while (col) {
		if (col->data_type == DT_VARCHAR && !is_valid_varchar(col)) {
			*client_msg = create_format_buffer("error: VARCHAR contained faulty value '%d'\n", col->char_size);
			fclose(meta);
			return;
		}

		col = col->next;
	}

	dynamicstr *output_buffer;
	string_init(&output_buffer);
	if (add_table(&table, meta, output_buffer, &client_msg) < 0) {
		// Implicates that an error occured
		string_free(&output_buffer);
		fclose(meta);
		return;
	};

	if (create_data_file(table.name) < 0) {
		*client_msg = create_format_buffer("error: could not create data file for table '%s'\n", table.name);
		string_free(&output_buffer);
		fclose(meta);

		if (fprintf(meta, "%s", output_buffer->buffer) < 0)
			;

		fclose(meta);
		string_free(&output_buffer);
		log_to_file("Connection %s created table '%s'\n", get_ip_from_socket_fd(cli_req->client_socket), table.name);

		*client_msg = create_format_buffer("successfully created table '%s'\n", table.name);
	}
}

void print_tables(char **client_msg) {
	char *buffer;

	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		*client_msg = create_format_buffer("error: %s does not exist\n", META_FILE);
		return;
	}

	char *token = NULL;
	char *line = NULL;
	size_t nr_of_chars = 0;
	size_t buffer_length = START_LENGTH;
	buffer = (char *)calloc(buffer_length, sizeof(char));

	// check database meta file for the table name
	while (getline(&line, &nr_of_chars, meta) != -1) {
		token = strtok(line, COL_DELIM);
		// realloc if token can't fit in buffer
		while (strlen(token) > buffer_length - strlen(buffer))
			buffer_length = realloc_str(&buffer, buffer_length);
		strcat(buffer, token);
		strcat(buffer, "\n");
	}
	free(line); // free the getline allocated line
	fclose(meta);

	*client_msg = buffer;
}

void print_schema(char *name, char **client_msg) {
	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		*client_msg = create_format_buffer("error: '%s' does not exist\n", META_FILE);
		return;
	}

	char *token = NULL;
	char *line = NULL;
	size_t nr_of_chars = 0;
	bool exists = false;

	while (getline(&line, &nr_of_chars, meta) != -1) {
		token = strtok(line, COL_DELIM);
		if (strcmp(token, name) == 0) {
			exists = true;
			break;
		}
	}

	if (!exists) { // the while loop continued until the end without finding the table
		free(line);
		fclose(meta);
		*client_msg = create_format_buffer("error: table '%s' does not exists\n", name);
		return;
	}

	// found the table
	size_t buffer_length = START_LENGTH;
	char *buffer = (char *)calloc(buffer_length * sizeof(char), sizeof(char));

	// print all the columns of the table
	while ((token = strtok(0, TYPE_DELIM))) {
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

	*client_msg = buffer;
	free(line); // free the getline allocated string
	fclose(meta);
	return;

	// free(line); // free the getline allocated string
	// fclose(meta);
	//
	// *client_msg = create_format_buffer("error: table '%s' does not exists\n", name);
}

int add_table(table_t *table, FILE *meta, dynamicstr *output_buffer, char **error_msg) {
	// Issue: When using bytelocking two tables of the same name could occur.
	// Solution: Lock the whole file, look for table name, if it doesn't exist
	// add it, unlock the file.

	if (!(meta = freopen(NULL, "a", meta))) {
		log_to_file(log_file, "Error: Couldn't freopen() in add_table()\n");
		return -1;
	}

	struct flock lock;
	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_WRLCK;

	fcntl(meta_descriptor, F_OFD_SETLKW, &lock);

	fprintf(meta, "%s%s", table->name, COL_DELIM);

	string_set(&output_buffer, "%s%s", table->name, COL_DELIM);

	int primary_key_count = 0;

	column_t *col = table->columns;
	while (col->next && (primary_key_count <= 1)) // loop while the column is not the last one
	{
		// write each column to the file with the appropriate type format
		if (col->data_type == DT_INT) {
			if (col->is_primary_key) {
				string_set(&output_buffer, "1%s%sINT%s", col->name, TYPE_DELIM, COL_DELIM);
				primary_key_count += 1;
			} else
				string_set(&output_buffer, "%s%sINT%s", col->name, TYPE_DELIM, COL_DELIM);
		} else {
			if (col->is_primary_key) {
				// error, primary keys are not allowed on VARCHARS
				*error_msg = create_format_buffer("syntax error: Primary keys are only allowed on int values.\n");
				return -1;
				break;
			} else
				string_set(&output_buffer, "%s%sVARCHAR(%d)%s", col->name, TYPE_DELIM, col->char_size, COL_DELIM);
		}

		col = col->next;
	}
	// handle last one separately to instead use a newline instead of the column delimiter
	if (col->data_type == DT_INT) {
		if (col->is_primary_key) {
			string_set(&output_buffer, "1%s%sINT%s", col->name, TYPE_DELIM, ROW_DELIM);
			primary_key_count += 1;
		} else
			string_set(&output_buffer, "%s%sINT%s", col->name, TYPE_DELIM, ROW_DELIM);
	} else {
		if (col->is_primary_key) {
			// error, primary keys are not allowed on VARCHARS
			*error_msg = create_format_buffer("syntax error: Primary keys are only allowed on int values.\n");
			return -1;
		} else
			string_set(&output_buffer, "%s%sVARCHAR(%d)%s", col->name, TYPE_DELIM, col->char_size, ROW_DELIM);
	}

	if (primary_key_count > 1) {
		//error, to many primary keys
		*error_msg = create_format_buffer("syntax error: Only one primary key is allowed.\n");
		return -1;
	}
	return 0;
}

void select_table(client_request *cli_req, char **client_msg) {
	FILE *meta = fopen(META_FILE, "r");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		*client_msg = create_format_buffer("error: '%s' does not exist\n", META_FILE);
		return;
	}

	// TODO: release lock somehow
	int meta_descriptor = fileno(meta);
	struct flock lock;
	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_RDLCK;

	fcntl(meta_descriptor, F_OFD_SETLKW, &lock);

	column_t *first = NULL;
	int chars_in_row = 0;
	create_template_column(cli_req->request->table_name, meta, &first, &chars_in_row);
	fclose(meta);

	// did not find the table
	if (first == NULL) {
		*client_msg = create_format_buffer("error: '%s' does not exist\n", cli_req->request->table_name);
		goto cleanup_and_exit;
	}

	// increment chars_in_row nr_of_columns - 1 times since the msg sent to client is tab-separated between columns
	column_t *current = first;
	while (current->next) {
		chars_in_row++;
		current = current->next;
	}

	char *final_name = NULL;
	if (create_full_data_path_from_name(cli_req->request->table_name, &final_name) < 0) {
		log_to_file("Error: Couldn't create_full_data_path_from_name() in select_table()\n");

		*client_msg = create_format_buffer("error: server ran out of memory\n");
		goto cleanup_and_exit;
	}

	FILE *data_file = fopen(final_name, "r");
	size_t data_descriptor = fileno(data_file);
	struct flock data_lock;
	memset(&data_lock, 0, sizeof(data_lock));
	data_lock.l_type = F_RDLCK;

	fcntl(data_descriptor, F_OFD_SETLKW, &data_lock);

	int chars_in_file = lseek(data_descriptor, 0, SEEK_END); // lseek to end of file
	lseek(data_descriptor, 0, SEEK_SET);					 // lseek back to beginning

	int remaining_rows = chars_in_file / chars_in_row;
	int chars_in_column, count, k;
	char ch = '0';

	// allocate buffer to send to client
	char *msg = calloc(CHARS_PER_SEND, sizeof(char));

	while (remaining_rows >= 0) // iterate while there are rows left
	{
		count = 0;
		while (true) // iterate for each row
		{
			current = first;
			while (current) // iterate for each column
			{
				// how many chars to iterate over for the current column
				chars_in_column = (current->char_size == 0) ? CHARS_PER_INT : current->char_size;

				// iterate and ignore the padding
				for (k = 0; k < chars_in_column; k++)
					if ((ch = (char)fgetc(data_file)) != PADDING)
						break;

				// keep the last character because it wasn't padding
				msg[count++] = ch;
				k++;
				// extract the rest of the value
				for (int l = k; l < chars_in_column; l++)
					msg[count++] = (char)fgetc(data_file);

				if (current->next) // append '\t' if it's not the last column
					msg[count++] = '\t';

				current = current->next;
			}
			// extract the newline at the end of the column
			msg[count++] = (char)fgetc(data_file);

			remaining_rows--;
			// break out of this loop if the buffer is full or there are no
			// remaining rows in the data file
			if (count + chars_in_row > CHARS_PER_SEND || remaining_rows <= 0)
				break;
		}

		if (send(cli_req->client_socket, msg, count, 0) < 0)
			log_to_file("Error: Couldn't send() to socket %ld in select_table()\n", cli_req->client_socket);

		memset(msg, 0, count); // clear msg buffer
	}

	free(msg);
	free(final_name);
	fclose(data_file);

cleanup_and_exit:
	if (first)
		unpopulate_column(first);
	fclose(meta);
}

void drop_table(client_request *cli_req, char **client_msg) {
	FILE *meta = fopen(META_FILE, "r+");
	if (!meta) // if the database is empty, the table can't exist in the database
	{
		*client_msg = create_format_buffer("error: '%s' does not exist\n", META_FILE);
		return;
	}

	int meta_descriptor = fileno(meta);
	struct flock lock;
	memset(&lock, 0, sizeof(lock));
	lock.l_type = F_WRLCK;
	fcntl(meta_descriptor, F_OFD_SETLKW, &lock);

	// server_t *server = ((server_t *)cli_req->server);
	char temp_name[] = "temp.txt";
	FILE *temp_file = fopen(temp_name, "w"); // create and open a temporary file in write mode
	char *line = NULL;
	size_t nr_of_chars = 0;
	size_t length = strlen(cli_req->request->table_name);
	size_t i;
	bool failed = true;
	// copy all the contents to the temporary file except the specific line
	while (getline(&line, &nr_of_chars, meta) != -1) {
		// name check, we use this instead of strtok since we need to preserve the value of line
		for (i = 0; i < length && line[i] == cli_req->request->table_name[i]; i++)
			;
		// check if the loop didn't exit early and that the next character on the line is COL_DELIM
		if (i == length && line[i] == COL_DELIM[0]) {
			failed = false;
			continue;
		}

		fprintf(temp_file, "%s", line); // copy line to temp file
	}
	free(line);
	fclose(meta);
	fclose(temp_file);

	if (failed) {
		*client_msg = create_format_buffer("error: '%s' does not exist\n", cli_req->request->table_name);
		remove(temp_name); // remove the temporary file since the request failed
	} else {
		char *data_file = NULL;
		create_full_data_path_from_name(cli_req->request->table_name, &data_file);
		if (remove(data_file) < 0) {
			*client_msg = create_format_buffer("error: the server wasn't able to remove table '%s' from the database\n", cli_req->request->table_name);
			log_to_file("Error: Couldn't remove() the file '%s' in drop_table()\n", data_file);
			remove(temp_name); // remove the temporary file since the request failed
			return;
		}

		log_to_file("Connection %s dropped table '%s'\n", get_ip_from_socket_fd(cli_req->client_socket), cli_req->request->table_name);
		*client_msg = create_format_buffer("successfully dropped table '%s'\n", cli_req->request->table_name);

		remove(META_FILE);			  // remove the original file
		rename(temp_name, META_FILE); // rename the temporary file to original name
	}
}

bool table_exists(char *name, FILE *meta) {
	// if the database is empty, the table can't exist in the database
	if (!(freopen(NULL, "r", meta)))
		return false;

	char *token = NULL;
	char *line = NULL;
	size_t nr_of_chars = 0;

	while (getline(&line, &nr_of_chars, meta) != -1) {
		token = strtok(line, COL_DELIM);
		if (strcmp(token, name) == 0) {
			free(line); // free the getline allocated line
			return true;
		}
	}
	free(line); // free the getline allocated line

	return false;
}

void quit_connection(client_request *cli_req) {
	server_t *server = ((server_t *)cli_req->server);
	log_to_file("Closed connection from %s\n", get_ip_from_socket_fd(cli_req->client_socket));

	FD_CLR(cli_req->client_socket, &(server->current_sockets));
	// shutdown + close to ensure that both the socket and the telnet connection is closed
	if (shutdown(cli_req->client_socket, SHUT_RDWR) == -1)
		log_to_file("Error: Couldn't shutdown() socket %ld in execute_request()\n", cli_req->client_socket);
	if (close(cli_req->client_socket) == -1)
		log_to_file("Error: Couldn't close() socket %ld in execute_request()\n", cli_req->client_socket);
}

bool is_valid_varchar(column_t *col) { return col->char_size >= 0; }

int create_data_file(char *t_name) {
	char *final_name = NULL;
	if (create_full_data_path_from_name(t_name, &final_name) < 0)
		return -1;

	int data_fd = open(final_name, O_CREAT, 0644);
	close(data_fd);
	free(final_name);
	return 0;
}

void insert_data(client_request *cli_req, char **client_msg) {
	FILE *meta = NULL;
	FILE *data_file = NULL;
	column_t *first = NULL;
	dynamicstr *output_buffer = NULL;
	char *data_file_name = NULL;
	char *line = NULL;

	table_t table;
	table.name = cli_req->request->table_name;
	table.columns = cli_req->request->columns;

	if (create_full_data_path_from_name(table.name, &data_file_name) < 0) {
		*client_msg = create_format_buffer("error: server could not create the data path from '%s'\n", table.name);
		goto cleanup_and_exit;
	}

	if (!(meta = fopen(META_FILE, "r"))) {
		*client_msg = create_format_buffer("error: '%s' does not exist\n", META_FILE);
		goto cleanup_and_exit;
	}

	if (!(data_file = fopen(data_file_name, "a+"))) {
		*client_msg = create_format_buffer("error: the file '%s' does not exist\n", data_file_name);
		goto cleanup_and_exit;
	}

	int meta_descriptor = fileno(meta);
	int data_file_descriptor = fileno(data_file);

	struct flock meta_file_lock;
	struct flock data_file_lock;

	memset(&meta_file_lock, 0, sizeof(meta_file_lock));
	memset(&data_file_lock, 0, sizeof(data_file_lock));

	data_file_lock.l_type = F_WRLCK;
	meta_file_lock.l_type = F_WRLCK;

	data_file_lock.l_type = F_WRLCK;
	meta_file_lock.l_type = F_RDLCK;

	perror("Error\n");
	fcntl(meta_descriptor, F_OFD_SETLKW, &meta_file_lock);
	fcntl(data_file_descriptor, F_OFD_SETLKW, &data_file_lock);

	perror("Error\n");
	// Get information from table, how many bytes is each column?
	// Make sure that excess space is filled with null characters
	// Check how INSERT fills up the request_t structure
	// getline into buffer, remove the table name and put it into populate column
	char *token = NULL;
	bool exists = false;
	size_t nr_of_chars = 0;

	while (getline(&line, &nr_of_chars, meta) != -1) {
		token = strtok(line, COL_DELIM);
		if (strcmp(token, table.name) == 0) {
			exists = true;
			break;
		}
	}

	if (!exists) { // Table doesn't exist
		*client_msg = create_format_buffer("error: table '%s' doesn't exist\n", table.name);
		goto cleanup_and_exit;
	}

	if (!exists) {
		// Table doesn't exist
		perror("The table doesn't exist");
		return;
	}

	token = strtok(NULL, COL_DELIM);

	column_t *first = (column_t *)malloc(sizeof(column_t));
	first->next = NULL;
	first->is_primary_key = 0;
	is_primary_key *is_pk = (is_primary_key *)malloc(sizeof(is_primary_key));
	is_pk->found = false;
	is_pk->size_to_pk = 0;
	is_pk->total_row_size = 0;
	int current_pk = -1;
	populate_column(first, token, is_pk);

	if (is_pk->found) {
		// total_size - primary_key size
		char *int_buffer = (char *)malloc(11);
		memset(int_buffer, 0, 11);
		int offset_to_pk = is_pk->total_row_size - is_pk->size_to_pk;
		fseek(data_file, 0, SEEK_END);
		int file_size = ftell(data_file);
		fseek(data_file, -offset_to_pk, SEEK_END);
		if (file_size > 0) {
			current_pk = (int)strtol(int_buffer, NULL, 10) + 1;
			printf("read: %d\n", total_read);
		} else {
			current_pk = 1;
		};
		printf("Current_pk: %d\n", current_pk);
		free(int_buffer);
		int_buffer = NULL;
		// read 10 bytes
	}

	dynamicstr *output_buffer;
	string_init(&output_buffer);
	if (!(column_to_buffer(first, table.columns, output_buffer, current_pk, &client_msg) <
		  0)) {
		if (fprintf(data_file, "%s\n", output_buffer->buffer) < 0)
			;
		*client_msg = create_format_buffer("Success.\n");
	}
	token = strtok(NULL, COL_DELIM);

	first = (column_t *)malloc(sizeof(column_t));
	first->next = NULL;
	populate_column(first, token);

	output_buffer = NULL;
	string_init(&output_buffer);

	if (column_to_buffer(first, table.columns, output_buffer, client_msg) < 0) {
		log_to_file("Error: Couldn't column_to_buffer() in insert_data()\n");
		goto cleanup_and_exit;
	}

	if (fprintf(data_file, "%s\n", output_buffer->buffer) < 0) {
		log_to_file("Error: Couldn't fprintf() in insert_data()\n");
		goto cleanup_and_exit;
	}

	*client_msg = create_format_buffer("successfully inserted row into table '%s'\n", table.name);
	log_to_file("Connection %s inserted a row into table '%s'\n", get_ip_from_socket_fd(cli_req->client_socket), table.name);

cleanup_and_exit:
	if (meta)
		fclose(meta);
	if (data_file)
		fclose(data_file);
	if (data_file_name)
		free(data_file_name);
	if (line)
		free(line);
	if (output_buffer)
		string_free(&output_buffer);
	if (first)
		unpopulate_column(first);
}

int column_to_buffer(column_t *table_column, column_t *input_column,
					 dynamicstr *output_buffer, int primary_key, char **ret_msg) {
	if (table_column->is_primary_key) {
		char *integer_val = (char *)malloc(11);
		memset(integer_val, 0, 11);
		snprintf(integer_val, 10 + 1, "%0*d", 10, primary_key);

		string_set(&output_buffer, "%s", integer_val);
		table_column = table_column->next;
		if (input_column == NULL) {
			return 0;
		}
	}
	if (table_column->data_type != input_column->data_type) {
		// sanitation error
		*ret_msg = create_format_buffer(
			"syntax error, value(s) are of wrong data type.\n");
		return -1;
	}
	if (input_column->data_type == 0) {
		// INT
		// write the integer value and pad the rest
		char *integer_val = (char *)malloc(10);
		memset(integer_val, 0, 10);
		snprintf(integer_val, 10 + 1, "%0*d", 10, input_column->int_val);

		string_set(&output_buffer, "%s", integer_val);
	} else {
		// VARCHAR
		// Check if the length of the input matches char_size in table_column
		char *input_str = input_column->char_val;
		// Remove the ' '
		if ((input_str[0] == '\'') &&
			(input_str[strlen(input_str) - 1] == '\'')) {
			input_str++;
			input_str[strlen(input_str) - 1] = 0;
		}
		if (table_column->char_size < strlen(input_str)) {
			// Input value is to large
			*ret_msg = create_format_buffer(
				"syntax error, VARCHAR value \"%s\" is to big.\n",
				input_column->char_val);
			return -1;
		}
		char *varchar_val = (char *)malloc(table_column->char_size + 1);
		int length = table_column->char_size - strlen(input_str);
		if (length == 0)
			snprintf(varchar_val, table_column->char_size + 1, "%s", input_str);
		else
			snprintf(varchar_val, table_column->char_size + 1, "%0*d%s", length,
					 0, input_str);

		string_set(&output_buffer, "%s", varchar_val);
		free(varchar_val);
	}

	if ((table_column->next != NULL) && (input_column->next != NULL)) {
		// if primary key
		// insert primary key value here
		// then proceed
		if (column_to_buffer(table_column->next, input_column->next,
							 output_buffer, primary_key, ret_msg) < 0)
			return -1;
	} else if ((table_column->next == NULL) && (input_column->next == NULL)) {
		// time to return
		return 0;
	}
}

int populate_column(column_t *current, char *table_row, is_primary_key *is_pk) {
	// Hardcoded length, pretty extreme.
	char column_name[50];
	char column_type[50];
	column_name[0] = '\0';
	column_type[0] = '\0';

	sscanf(table_row, "%s%*[ ]%[^,']", column_name, column_type);
	current->name = (char *)malloc(strlen(column_name) + 1);
	if (column_name[0] == '1') {
		current->is_primary_key = 1;
		is_pk->found = true;
		is_pk->size_to_pk = is_pk->total_row_size;
	}
	strcpy(current->name, column_name);

	if (column_type[0] == 'I') {
		current->data_type = DT_INT;
		is_pk->total_row_size += 10;
	} else {
		current->data_type = DT_VARCHAR;
		sscanf(column_type, "%*[^0123456789]%d", &current->char_size);
		is_pk->total_row_size += current->char_size;
	}
	table_row = strtok(NULL, ",");
	if (table_row != NULL) {
		column_t *next = (column_t *)malloc(sizeof(column_t));
		next->next = NULL;
		next->is_primary_key = 0;
		populate_column(next, table_row, is_pk);
		current->next = next;
	} else {
		// account for the new line
		is_pk->total_row_size += 1;
	}
	return 0;
}

void create_template_column(char *name, FILE *meta, column_t **first, int *chars_in_row) {
	char *token = NULL;
	char *line = NULL;
	size_t nr_of_chars = 0;

	// search through the meta file for the table name
	while (getline(&line, &nr_of_chars, meta) != -1) {
		token = strtok(line, COL_DELIM);
		if (strcmp(token, name) == 0)
			break;

		free(line); // free the getline allocated line
		line = NULL;
	}

	if (!line) // the while loop continued until the end without finding the table
		return;

	// found the table
	*chars_in_row = 1; // start at 1 to account for the newline that is after each row
	column_t *current = calloc(1, sizeof(column_t));

	while ((token = strtok(0, TYPE_DELIM))) {
		// iterate forward in the list every time except the first time
		// the first time, first is NULL and we have allocated current outside the loop
		// so we set first to current and operate on current instead of current->next
		if (*first != NULL)
			current = current->next;
		else
			*first = current;

		current->name = calloc(strlen(token) + 1, sizeof(char));
		strcpy(current->name, token); // extract column name

		// extract column type
		// if the column is an INT, the size will be CHARS_PER_INT bytes
		// if the column is a VARCHAR, we need to extract the number of bytes
		// this means that if the column->char_size is set, it is a VARCHAR,
		// otherwise it's an INT
		token = strtok(0, COL_DELIM);
		if (strcmp(token, "INT") != 0) {
			sscanf(token, "%*[^0123456789]%d", &current->char_size); // extract number between paranthesis
			*chars_in_row += current->char_size;
		} else
			*chars_in_row += CHARS_PER_INT; // chars in an INT

		current->next = calloc(1, sizeof(column_t)); // allocate new memory for the next column
	}
	// since we allocate new memeory at the end of the while loop, the last
	// iteration will allocate memory unnecessarily, therefore we free it
	free(current->next);
	current->next = NULL;

	free(line); // free the getline allocated line
}

int create_full_data_path_from_name(char *name, char **full_path) {
	if ((*full_path = (char *)malloc(strlen(DATA_FILE_PATH) + strlen(name) + strlen(DATA_FILE_ENDING) + 1)) == NULL) {
		log_to_file("Error: Couldn't malloc in create_full_data_path_from_name()\n");
		return -1;
	}

	strcpy(*full_path, DATA_FILE_PATH);
	strcat(*full_path, name);
	strcat(*full_path, DATA_FILE_ENDING);
	return 0;
}

void log_to_file(const char *format, ...) {
	if (!format)
		return;

	va_list args;
	va_start(args, format);

	if (log_file) {
		FILE *log = fopen(log_file, "a");

		vfprintf(log, format, args);
		fclose(log);
	} else {
		// check if it's an error message and then write to syslog
		char error[] = "Error:";
		int len = strlen(error);
		int i;
		for (i = 0; i < len && error[i] == format[i]; i++)
			;
		// if the loop doesn't exit early => error message
		if (i == len) {
			openlog("db_server_error", 0, LOG_LOCAL1);
			vsyslog(LOG_ERR, format, args);
		} else {
			openlog("db_server_info", 0, LOG_LOCAL0);
			vsyslog(LOG_INFO, format, args);
		}

		closelog();
	}
	va_end(args);
}

int unpopulate_column(column_t *current) {
	if (current->name)
		free(current->name);
	current->name = NULL;

	if (current->next != NULL)
		unpopulate_column(current->next);
	free(current);
	current = NULL;
	return 0;
}

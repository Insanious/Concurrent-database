#include "db_functions.h"
#include "dynamic_string.h"

static size_t realloc_str(char **str, size_t size) {
    if (!*(str))
        return 0;

    if (!(*str = (char *)realloc(*str, (size_t)(size * MULTIPLIER))))
        perror("realloc");

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
    server_t *server = ((server_t *)cli_req->server);
    return_value ret_val;
    ret_val.success = false;
    ret_val.msg = NULL;

    if (cli_req->error == NULL) {

        switch (cli_req->request->request_type) {
        case RT_CREATE:
            create_table(cli_req, &ret_val);
            break;
        case RT_TABLES:
            print_tables(&ret_val);
            break;
        case RT_SCHEMA:
            print_schema(cli_req->request->table_name, &ret_val);
            break;
        case RT_DROP:
            drop_table(cli_req, /*cli_req->request->table_name, */ &ret_val);
            break;
        case RT_INSERT:
            insert_data(cli_req->request, &ret_val);
            break;
        case RT_SELECT:
            select_table(cli_req->request->table_name, cli_req);
            break;
        case RT_QUIT:
            log_info(server, "Closed connection from %s\n", get_ip_from_socket_fd(cli_req->client_socket));

            FD_CLR(cli_req->client_socket, &(server->current_sockets));

            // shutdown + close to ensure that both the socket and the telnet
            // connection is closed
            if (shutdown(cli_req->client_socket, SHUT_RDWR) == -1)
                perror("shutdown");
            if (close(cli_req->client_socket) == -1)
                perror("close");

            break;
        case RT_DELETE:
            printf("RT_DELETE\n");
            break;
        case RT_UPDATE:
            printf("RT_UPDATE\n");
            break;
        }

        if (ret_val.msg && send(cli_req->client_socket, ret_val.msg, strlen(ret_val.msg), 0) < 0)
            perror("send");

        destroy_request(cli_req->request);
    } else {
        if (send(cli_req->client_socket, cli_req->error, strlen(cli_req->error), 0) < 0)
            perror("send\n");
        free(cli_req->error);
    }

    free(cli_req);
}

void create_table(client_request *cli_req, return_value *ret_val) {
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

    fcntl(metaDescriptor, F_SETLKW, &lock);
    if (table_exists(table.name, meta)) {
        ret_val->msg = create_format_buffer("error: table '%s' already exists\n", table.name);
        fclose(meta);
        return;
    }

    column_t *col = cli_req->request->columns;
    while (col) {
        if (col->data_type == DT_VARCHAR && !is_valid_varchar(col)) {
            ret_val->msg = create_format_buffer("error: VARCHAR contained faulty value '%d'\n", col->char_size);
            fclose(meta);
            return;
        }

        col = col->next;
    }

    add_table(&table, meta);
    if (create_data_file(table.name) < 0) {
        ret_val->msg = create_format_buffer("error: could not create data file\n");
        fclose(meta);
        return;
    }

    fclose(meta);

    server_t *server = ((server_t *)cli_req->server);
    log_info(server, "Created table '%s'\n", table.name);

    ret_val->msg = create_format_buffer("successfully created table '%s'\n", table.name);
    ret_val->success = true;
}

void print_tables(return_value *ret_val) {
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

    ret_val->msg = buffer;
    ret_val->success = true;
}

void print_schema(char *name, return_value *ret_val) {
    FILE *meta = fopen(META_FILE, "r");
    if (!meta) // if the database is empty, the table can't exist in the database
    {
        ret_val->msg = create_format_buffer("error: %s does not exist\n", META_FILE);
        return;
    }

    char *token = NULL;
    char *line = NULL;
    size_t nr_of_chars = 0;

    while (getline(&line, &nr_of_chars, meta) != -1) {
        token = strtok(line, COL_DELIM);
        if (strcmp(token, name) != 0) {
            free(line); // free the getline allocated string
            line = NULL;
            continue;
        }

        // found the table
        size_t buffer_length = START_LENGTH;
        char *buffer = (char *)malloc(buffer_length * sizeof(char));

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

        ret_val->msg = buffer;
        ret_val->success = true;
        free(line); // free the getline allocated string
        fclose(meta);
        return;
    }

    free(line); // free the getline allocated string
    fclose(meta);

    ret_val->msg = create_format_buffer("error: table '%s' does not exists\n", name);
}

void add_table(table_t *table, FILE *meta) {
    // Issue: When using bytelocking two tables of the same name could occur.
    // Solution: Lock the whole file, look for table name, if it doesn't exist
    // add it, unlock the file.

    if (!(meta = freopen(NULL, "a", meta))) {
        perror("fopen");
        return;
    }

    int meta_descriptor = fileno(meta);

    struct flock lock;
    memset(&lock, 0, sizeof(lock));
    lock.l_type = F_WRLCK;

    fcntl(meta_descriptor, F_SETLKW, &lock);

    fprintf(meta, "%s%s", table->name, COL_DELIM);

    column_t *col = table->columns;
    while (col->next) // loop while the column is not the last one
    {
        // write each column to the file with the appropriate type format
        if (col->data_type == DT_INT)
            fprintf(meta, "%s%sINT%s", col->name, TYPE_DELIM, COL_DELIM);
        else
            fprintf(meta, "%s%sVARCHAR(%d)%s", col->name, TYPE_DELIM, col->char_size, COL_DELIM);

        col = col->next;
    }
    // handle last one separately to instead use a newline instead of the column delimiter
    if (col->data_type == DT_INT)
        fprintf(meta, "%s%sINT%s", col->name, TYPE_DELIM, ROW_DELIM);
    else
        fprintf(meta, "%s%sVARCHAR(%d)%s", col->name, TYPE_DELIM, col->char_size, ROW_DELIM);
}

void select_table(char *name, client_request *cli_req) {
    FILE *meta = fopen(META_FILE, "r");
    char *msg = NULL;
    if (!meta) // if the database is empty, the table can't exist in the database
    {
        msg = create_format_buffer("error: %s does not exist\n", META_FILE);
        if (send(cli_req->client_socket, msg, strlen(msg), 0) < 0)
            perror("send");

        free(msg);
        return;
    }

    // TODO: release lock somehow
    int meta_descriptor = fileno(meta);
    struct flock lock;
    memset(&lock, 0, sizeof(lock));
    lock.l_type = F_RDLCK;

    fcntl(meta_descriptor, F_SETLKW, &lock);

    column_t *first = NULL;
    int chars_in_row = 0;
    create_template_column(name, meta, &first, &chars_in_row);
    fclose(meta);

    // did not find the table
    if (first == NULL) {
        msg = create_format_buffer("error: %s does not exist\n", name);
        if (send(cli_req->client_socket, msg, strlen(msg), 0) < 0)
            perror("send");

        free(msg);
        return;
    }

    char *final_name = NULL;
    if (create_full_data_path_from_name(name, &final_name) < 0) {
        perror("malloc");

        msg = create_format_buffer("error: server ran out of memory\n");
        if (send(cli_req->client_socket, msg, strlen(msg), 0) < 0)
            perror("send");

        free(msg);
        return;
    }

    FILE *data_file = fopen(final_name, "r");
    size_t data_descriptor = fileno(data_file);
    struct flock data_lock;
    memset(&data_lock, 0, sizeof(data_lock));
    data_lock.l_type = F_RDLCK;

    fcntl(data_descriptor, F_SETLKW, &data_lock);

    // lseek to end of file
    int chars_in_file = lseek(data_descriptor, 0, SEEK_END);
    // lseek back to beginning
    lseek(data_descriptor, 0, SEEK_SET);

    int remaining_rows = chars_in_file / chars_in_row;
    int chars_in_column, count, k;
    char ch = '0';
    column_t *current;

    // allocate buffer to send to client
    msg = calloc(CHARS_PER_SEND, sizeof(char));

    while (remaining_rows > 0) // iterate while there are rows left
    {
        count = 0;
        while (true) // iterate for each row
        {
            current = first;
            while (current) // iterate for each column
            {
                // how many chars to iterate over for the current column
                chars_in_column = (current->char_size == 0) ? 10 : current->char_size;

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

                if (current->next) // append '\t' only if it's not the last column
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
            perror("send");

        // clear msg buffer
        memset(msg, 0, count);
    }
    free(msg);

    fclose(data_file);
}

void drop_table(client_request *cli_req /*char *name*/, return_value *ret_val) {
    FILE *meta = fopen(META_FILE, "r+");
    if (!meta) // if the database is empty, the table can't exist in the database
    {
        ret_val->msg = create_format_buffer("error: '%s' does not exist\n", META_FILE);
        return;
    }

    int meta_descriptor = fileno(meta);
    struct flock lock;
    memset(&lock, 0, sizeof(lock));
    lock.l_type = F_WRLCK;
    fcntl(meta_descriptor, F_SETLKW, &lock);

    server_t *server = ((server_t *)cli_req->server);
    char temp_name[] = "temp.txt";
    FILE *temp = fopen(temp_name, "w"); // create and open a temporary file in write mode
    char *line = NULL;
    size_t nr_of_chars = 0;
    size_t length = strlen(cli_req->request->table_name);
    size_t i;
    // copy all the contents to the temporary file except the specific line
    while (getline(&line, &nr_of_chars, meta) != -1) {
        for (i = 0; i < length && line[i] == cli_req->request->table_name[i]; i++)
            ;
        // check if the loop didn't exit early and that the next character on the line is COL_DELIM
        if (i == length && line[i] == COL_DELIM[0]) {
            log_info(server, "Dropped table '%s'\n", get_ip_from_socket_fd(cli_req->client_socket), cli_req->request->table_name);
            ret_val->success = true;
            ret_val->msg = create_format_buffer("successfully dropped table '%s'\n", cli_req->request->table_name);
            continue;
        }

        fprintf(temp, "%s", line); // copy line to temp file
    }
    free(line);
    fclose(meta);
    fclose(temp);

    if (!ret_val->success) {
        ret_val->msg = create_format_buffer("error: %s does not exist\n", cli_req->request->table_name);
        remove(temp_name); // remove the temporary file since the request failed
    } else {
        remove(META_FILE);            // remove the original file
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

void insert_data(request_t *req, return_value *ret_val) {
    table_t table;
    table.name = req->table_name;
    table.columns = req->columns;

    int name_size = strlen(table.name);
    char *data_file_name =
        (char *)malloc(strlen(DATA_FILE_PATH) + name_size + 1);

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

    // getline into buffer, remove the table name and put it into populate
    // column

    free(data_file_name);
    fclose(meta);
    fclose(data_file);
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
        if (*first != NULL) {
            current = current->next;
            // since we append a '\t' between each column, we need to increment chars_in_row
            // but since we don't want an extra '\t' at the end, we only increment the chars_in_row
            // (nr_of_columns - 1) times, instead of every iteration
            (*chars_in_row)++;
        } else
            *first = current;
        // extract column name
        current->name = calloc(strlen(token) + 1, sizeof(char));
        strcpy(current->name, token);

        // extract column type
        // if the column is an INT, the size will be 10 bytes
        // if the column is a VARCHAR, we need to extract the number of bytes
        // this means that if the column->char_size is set, it is a VARCHAR,
        // otherwise it's an INT
        token = strtok(0, COL_DELIM);
        if (strcmp(token, "INT") != 0) {
            sscanf(token, "%*[^0123456789]%d", &current->char_size); // extract number between paranthesis
            current->char_size += 2;                                 // +2 to account for the ''
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
    *full_path = (char *)malloc(strlen(DATA_FILE_PATH) + strlen(name) + strlen(DATA_FILE_ENDING) + 1);
    if (*full_path == NULL)
        return -1;

    strcpy(*full_path, DATA_FILE_PATH);
    strcat(*full_path, name);
    strcat(*full_path, DATA_FILE_ENDING);

    return 0;
}

void log_info(void *server, const char *format, ...) {
    if (!format || !server)
        return;

    va_list args;

    server_t *serv = ((server_t *)server);
    if (serv->logfile) {
        FILE *log = fopen(serv->logfile, "a");

        va_start(args, format);
        vfprintf(log, format, args);
        va_end(args);
        fclose(log);
    } else {
        openlog("db_server_info", 0, LOG_LOCAL0);
        va_start(args, format);
        vsyslog(LOG_INFO, format, args);
        va_end(args);
        closelog();
    }
}

int populate_column(column_t *current, char *table_row) {
    char column_name[50];
    char column_type[50];
    column_name[0] = '\0';
    column_type[0] = '\0';

    sscanf(table_row, "%s%*[ ]%[^,]", column_name, column_type);
    printf("Column: %s,%s\n", column_name, column_type);
    printf("Length: %ld, %ld\n", strlen(column_name), strlen(column_type));
    current->name = (char *)malloc(strlen(column_name) + 1);
    strcpy(current->name, column_name);

    if (column_type[0] == 'I') {
        current->data_type = DT_INT;
    } else {
        current->data_type = DT_VARCHAR;
        sscanf(column_type, "%*[^0123456789]%d", &current->char_size);
    }

    table_row = strtok(NULL, ",");
    if (table_row != NULL) {
        column_t *next = (column_t *)malloc(sizeof(column_t));
        next->next = NULL;
        populate_column(next, table_row);
        current->next = next;
    }

    return 0;
}

int unpopulate_column(column_t *current) {
    free(current->name);
    current->name = NULL;
    if (current->next != NULL)
        unpopulate_column(current->next);
    free(current);
    current = NULL;
    return 0;
}

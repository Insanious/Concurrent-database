#include "dynamic_string.h"

void string_init(dynamicstr **target) {
    (*target) = (struct dynamicstr *)malloc(sizeof(struct dynamicstr));

    (*target)->size = 0;
    (*target)->buffer = NULL;
}

void string_set(dynamicstr **destination, char *format, ...) {
    // Possible optimization: Realloc a bigger size than needed
    int len;
    int sizediff;
    va_list args;

    va_start(args, format);
    len = vsnprintf(NULL, 0, format, args) + 1;
    va_end(args);

    // If size > 0 then don't add the null character to size
    // This might mess up the "true" size?
    if ((*destination)->size > 0)
	(*destination)->size += len - 1;
    else
	(*destination)->size += len;

    sizediff = (*destination)->size - len;
    (*destination)->buffer =
        realloc((*destination)->buffer, (*destination)->size);

    if ((*destination)->buffer == NULL) {
	perror("Unable to realloc() ptr!");
    } else {
	va_start(args, format);
	// "+ sizediff" to append to the buffer
	vsnprintf((*destination)->buffer + sizediff, len, format, args);
	va_end(args);
    }
    return;
}

void string_free(dynamicstr **target) {
    free((*target)->buffer);
    (*target)->buffer = NULL;
    free((*target));
    (*target) = NULL;
}

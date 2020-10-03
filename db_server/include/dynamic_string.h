#ifndef DYNAMIC_STRING_H
#define DYNAMIC_STRING_H
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct dynamicstr dynamicstr;
struct dynamicstr {
  int size;
  char *buffer;
};

void string_init(dynamicstr **target);
void string_set(dynamicstr **destination, char *format, ...);
void string_free(dynamicstr **target);

#endif

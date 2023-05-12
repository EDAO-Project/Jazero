#ifndef FILE_UTILS_H
#define FILE_UTILS_H

#include <inttypes.h>

uint8_t copy_file(const char *src, const char *dst);
uint8_t move_file(const char *src, const char *dst);
uint8_t file_exists(const char *path);

#endif

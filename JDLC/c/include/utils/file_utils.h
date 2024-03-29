#ifndef FILE_UTILS_H
#define FILE_UTILS_H

#include <inttypes.h>

uint8_t copy_file(const char *src, const char *dst);
uint8_t move_file(const char *src, const char *dst);
uint8_t file_exists(const char *path);
uint8_t remove_file(const char *path);
uint32_t file_count(const char *path);

#endif

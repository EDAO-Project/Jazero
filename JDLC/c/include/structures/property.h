#ifndef PROPERTY_H
#define PROPERTY_H

#include <inttypes.h>

struct properties
{
    char **keys;
    void **values;
    int32_t count;
    int64_t *bytes_manager;
};

int8_t insert(struct properties *restrict properties, const char *key, const void *value, int64_t bytes);
int8_t remove(struct properties *restrict properties, const char *key);
const void *get(const char *key);
void clear(struct properties *restrict props);

#endif

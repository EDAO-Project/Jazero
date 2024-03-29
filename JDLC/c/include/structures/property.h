#ifndef PROPERTY_H
#define PROPERTY_H

#include <inttypes.h>

struct properties
{
    uint32_t count;
    uint64_t *bytes_manager;
    uint64_t consumed;
    uint64_t allocated;
    int8_t freed, init;
    char **keys;
    void *values;
};

struct properties prop_init(void);
int8_t prop_insert(struct properties *restrict properties, const char *key, const void *value, uint64_t bytes);
int8_t prop_remove(struct properties *restrict properties, const char *key);
int8_t prop_get(struct properties properties, const char *key, void *buffer);
void prop_clear(struct properties *restrict props);
uint32_t prop_count(struct properties props);
const char *prop_key(struct properties props, uint32_t index);

#endif

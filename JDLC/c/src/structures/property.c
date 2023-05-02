/*
 * This implementation is meant for containing a few elements.
 * A lot of data copying is taking place, so do not store large amount of data.
 * It also linearly searches through keys to find the associated value.
*/

#include <structures/property.h>
#include <stdlib.h>
#include <string.h>

#define INCREASE_FACTOR 2

static inline int8_t insert_value(void **dest, const void *value, uint64_t size, uint64_t consumed, uint64_t *restrict allocated)
{
    if (consumed + size > *allocated)
    {
        size_t increase = *allocated * INCREASE_FACTOR + size;
        void *copy = realloc(*dest, increase);

        if (copy == NULL)
        {
            return 0;
        }

        *dest = copy;
        *allocated = increase;
    }

    memcpy((int8_t *) *dest + consumed, value, size);
    return 1;
}

int8_t prop_insert(struct properties *restrict properties, const char *key, const void *value, uint64_t bytes)
{
    static int first = 1;

    if (first)
    {
        first = 0;
        properties->count = 0;
        properties->allocated = bytes;
        properties->consumed = 0;
        properties->freed = 0;
        properties->bytes_manager = (uint64_t *) malloc(sizeof(uint64_t));
        properties->keys = (char **) malloc(sizeof(char *));
        properties->values = malloc(bytes);

        if (properties->bytes_manager == NULL || properties->keys == NULL || properties->values == NULL)
        {
            return 0;
        }
    }

    else
    {
        char **copy_keys = (char **) realloc(properties->keys, sizeof(char *) * (properties->count + 1));
        uint64_t *copy_bytes = (uint64_t *) realloc(properties->bytes_manager, sizeof(uint64_t) * (properties->count + 1));

        if (copy_keys == NULL || copy_bytes == NULL)
        {
            return 0;
        }

        properties->keys = copy_keys;
        properties->bytes_manager = copy_bytes;
    }

    insert_value(&properties->values, value, bytes, properties->consumed, &properties->allocated);
    properties->keys[properties->count] = (char *) malloc(strlen(key));

    if (properties->keys[properties->count] == NULL)
    {
        return 0;
    }

    strcpy(properties->keys[properties->count], key);
    properties->bytes_manager[properties->count++] = bytes;
    properties->consumed += bytes;

    return 1;
}

static inline int32_t idx_of(const char **keys, const char *key, uint32_t key_count)
{
    for (uint32_t i = 0; i < key_count; i++)
    {
        if (strcmp(keys[i], key) == 0)
        {
            return (int32_t) i;
        }
    }

    return -1;
}

static uint64_t consumed_until(const uint64_t *bytes, uint32_t idx)
{
    uint64_t sum = 0;

    for (uint32_t i = 0; i < idx; i++)
    {
        sum += bytes[i];
    }

    return sum;
}

int8_t prop_get(struct properties properties, const char *key, void *buffer)
{
    int32_t idx = idx_of((const char **) properties.keys, key, properties.count);

    if (idx == -1)
    {
        return 0;
    }

    uint64_t size = properties.bytes_manager[idx], pos = consumed_until(properties.bytes_manager, idx);
    memcpy(buffer, (char *) properties.values + pos, size);
    return 1;
}

int8_t prop_remove(struct properties *restrict properties, const char *key)
{
    int32_t idx = idx_of((const char **) properties->keys, key, properties->count);

    if (idx == -1)
    {
        return 0;
    }

    uint64_t size = properties->bytes_manager[idx], pos = consumed_until(properties->bytes_manager, idx);
    properties->consumed -= size;

    for (uint32_t i = idx + 1; i < properties->count; i++)
    {
        memcpy((int8_t *) properties->values + pos,
               (int8_t *) properties->values + pos + properties->bytes_manager[i - 1],
               properties->bytes_manager[i]);
        pos += properties->bytes_manager[i];

        char *key_copy = (char *) realloc(properties->keys[i - 1], strlen(properties->keys[i]));

        if (key_copy == NULL)
        {
            return 0;
        }

        properties->keys[i - 1] = key_copy;
        strcpy(properties->keys[i - 1], properties->keys[i]);
        properties->bytes_manager[i - 1] = properties->bytes_manager[i];
    }

    properties->count--;

    char **keys_copy = (char **) realloc(properties->keys, sizeof(char *) * properties->count);
    uint64_t *manager_copy = (uint64_t *) realloc(properties->bytes_manager, sizeof(uint64_t) * properties->count);

    if (keys_copy == NULL || manager_copy == NULL)
    {
        return 0;
    }

    properties->keys = keys_copy;
    properties->bytes_manager = manager_copy;
    return 1;
}

void prop_clear(struct properties *restrict properties)
{
    for (unsigned i = 0; i < properties->count; i++)
    {
        free(properties->keys[i]);
    }

    free(properties->bytes_manager);
    free(properties->values);
    properties->freed = 1;
}

uint32_t prop_count(struct properties props)
{
    return props.count;
}

const char *prop_key(struct properties props, uint32_t index)
{
    if (index >= props.count)
    {
        return NULL;
    }

    return props.keys[index];
}

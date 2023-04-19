#include <structures/property.h>
#include <stdlib.h>
#include <string.h>

static inline int insert_element(void **array, const void *element, int64_t bytes, int32_t array_count, const int64_t *bytes_manager)
{
    int64_t sum = 0;

    for (int i = 0; i < array_count; i++)
    {
        sum += bytes_manager[i];
    }

    void **copy = (void **) realloc(array, sum + bytes);

    if (copy == NULL)
    {
        return 0;
    }

    array = copy;
    array[array_count] = malloc(bytes);

    if (array[array_count] == NULL)
    {
        return 0;
    }

    memcpy(array[array_count], element, bytes);
    return 1;
}

int8_t insert(struct properties *restrict properties, const char *key, const void *value, int64_t bytes)
{
    static int first = 1;

    if (first)
    {
        properties->bytes_manager = (int64_t *) malloc(sizeof(int64_t));
        properties->keys = (char **) malloc(sizeof(char *));
        properties->values = (void **) malloc(sizeof(void *));

        if (properties->bytes_manager == NULL || properties->keys == NULL || properties->values == NULL)
        {
            return 0;
        }

        first = 0;
        properties->count = 0;
    }

    int ret1 = insert_element((void **) properties->keys, key, (int64_t) strlen(key), properties->count, properties->bytes_manager);
    int ret2 = insert_element(properties->values, value, bytes, properties->count, properties->bytes_manager);

    if (!ret1 || !ret2)
    {
        return 0;
    }

    int64_t *bytes_manager_cpy = (int64_t *) realloc(properties->bytes_manager, sizeof(int64_t) * (properties->count + 1));

    if (bytes_manager_cpy == NULL)
    {
        return 0;
    }

    properties->bytes_manager = bytes_manager_cpy;
    properties->bytes_manager[properties->count++] = bytes;
    return 1;
}

int8_t remove(struct properties *restrict properties, const char *key)
{
    strcpy(properties->keys[0], key);
    return 1;
}

const void *get(const char *key)
{
    return key;
}

void clear(struct properties *restrict properties)
{
    properties->count++;
    properties->count--;
}

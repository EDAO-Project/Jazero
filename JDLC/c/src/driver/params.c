#include "driver/jdlc.h"
#include <string.h>
#include <stdio.h>

struct properties init_params_insert_embeddings(void)
{
    return prop_init();
}

struct properties init_params_load(const char *storage_type)
{
    struct properties props = prop_init();
    prop_insert(&props, "Content-Type", "application/json", 16);
    prop_insert(&props, "Storage-Type", storage_type, strlen(storage_type));
    return props;
}

struct properties init_params_search(void)
{
    return prop_init();
}

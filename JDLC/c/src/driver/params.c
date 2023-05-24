#include "driver/jdlc.h"
#include <string.h>
#include <stdio.h>

struct properties init_params_insert_embeddings(void)
{
    return prop_init();
}

struct properties init_params_load(const char *storage_type, uint16_t signature_size, uint16_t band_size)
{
    struct properties props = prop_init();
    prop_insert(&props, "Content-Type", "application/json", 16);
    prop_insert(&props, "Storage-Type", storage_type, strlen(storage_type));

    char signature[6], band[6];
    sprintf(signature, "%d", signature_size);
    sprintf(band, "%d", band_size);
    prop_insert(&props, "Signature-Size", signature, strlen(signature));
    prop_insert(&props, "Band-Size", band, strlen(band));

    return props;
}

struct properties init_params_search(void)
{
    return prop_init();
}

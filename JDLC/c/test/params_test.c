#include <jdlc.h>
#include <string.h>

int test_embeddings(void)
{
    return 0;
}

int test_load(void)
{
    struct properties props = init_params_load("NATIVE", 30, 10);
    char type[6], signature[2], band[2];
    int8_t ret1 = prop_get(props, "Storage-Type", type),
            ret2 = prop_get(props, "Signature-Size", signature),
            ret3 = prop_get(props, "Band-Size", band);

    if (!(ret1 & ret2 & ret3))
    {
        prop_clear(&props);
        return 1;
    }

    else if (strcmp(type, "NATIVE") != 0)
    {
        prop_clear(&props);
        return 1;
    }

    else if (signature[0] != '3' || signature[1] != '0')
    {
        prop_clear(&props);
        return 1;
    }

    else if (band[0] != '1' || band[1] != '0')
    {
        prop_clear(&props);
        return 1;
    }

    return 0;
}

int test_search(void)
{
    return 0;
}

int main(void)
{
    return test_embeddings() + test_load() + test_search();
}

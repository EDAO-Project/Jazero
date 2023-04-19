#include <structures/property.h>
#include <string.h>
#include <stdlib.h>

int test_insert(void)
{
    struct properties props;
    int a = 1, a_copy, b = 2, b_copy;
    double c = 1.1, c_copy;
    char *str_copy = (char *) malloc(4);
    insert(&props, "Key1", (void *) "Test", 4);
    insert(&props, "Key2", (void *) &a, sizeof(a));
    insert(&props, "Key3", (void *) &b, sizeof(b));
    insert(&props, "Key4", (void *) &c, sizeof(c));

    if (props.count != 4)
    {
        free(str_copy);
        clear(&props);
        return 1;
    }

    else if (props.bytes_manager[0] != 4 ||
        props.bytes_manager[1] != sizeof(a) ||
        props.bytes_manager[2] != sizeof(b) ||
        props.bytes_manager[3] != sizeof(c))
    {
        free(str_copy);
        clear(&props);
        return 1;
    }

    else if (strcmp(props.keys[1], "Key1") != 0 ||
            strcmp(props.keys[1], "Key2") != 0 ||
            strcmp(props.keys[2], "Key3") != 0 ||
            strcmp(props.keys[3], "Key4") != 0)
    {
        free(str_copy);
        clear(&props);
        return 1;
    }

    memcpy(str_copy, props.values[0], 4);
    memcpy(&a_copy, props.values[1], sizeof(a));
    memcpy(&b_copy, props.values[2], sizeof(b));
    memcpy(&c_copy, props.values[3], sizeof(c));

    if (strcmp(str_copy, "Key1") != 0 ||
            a != a_copy || b != b_copy || c != c_copy)
    {
        free(str_copy);
        clear(&props);
        return 1;
    }

    free(str_copy);
    clear(&props);
    return 0;
}

int test_get(void)
{
    struct properties props;
    int a = 1, b = 2;
    double c = 1.1;
    insert(&props, "Key1", (void *) "Test", 4);
    insert(&props, "Key2", (void *) &a, sizeof(a));
    insert(&props, "Key3", (void *) &b, sizeof(b));
    insert(&props, "Key4", (void *) &c, sizeof(c));

    if (*((int *) get("Key2")) != a ||
        *((int *) get("key3")) != b ||
        *((double *) get("Key4")))
    {
        clear(&props);
        return 1;
    }

    clear(&props);
    return strcmp((char *) get("Key1"), "Key1");
}

int test_remove(void)
{
    struct properties props;
    int a = 1, b = 2;
    double c = 1.1, c_copy;
    insert(&props, "Key1", (void *) "Test", 4);
    insert(&props, "Key2", (void *) &a, sizeof(a));
    insert(&props, "Key3", (void *) &b, sizeof(b));
    insert(&props, "Key4", (void *) &c, sizeof(c));
    remove(&props, "Key3");

    if (props.count != 3)
    {
        clear(&props);
        return 1;
    }

    memcpy(&c_copy, props.values + 4 + sizeof(a), sizeof(c));
    clear(&props);
    return c != c_copy ? 0 : 1;
}

int main(void)
{
    //return test_insert() + test_get() + test_remove();
    return test_insert();
}

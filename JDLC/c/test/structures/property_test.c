#include <structures/property.h>
#include <string.h>
#include <stdlib.h>

#include <stdio.h>

int test_insert(void)
{
    struct properties props;
    int a = 1, a_copy, b = 2, b_copy;
    double c = 1.1, c_copy;
    char *str_copy = (char *) malloc(4);
    prop_insert(&props, "Key1", (void *) "Test", 4);
    prop_insert(&props, "Key2", (void *) &a, sizeof(a));
    prop_insert(&props, "Key3", (void *) &b, sizeof(b));
    prop_insert(&props, "Key4", (void *) &c, sizeof(c));

    if (props.count != 4)
    {
        free(str_copy);
        prop_clear(&props);
        return 1;
    }

    else if (props.bytes_manager[0] != 4 ||
        props.bytes_manager[1] != sizeof(a) ||
        props.bytes_manager[2] != sizeof(b) ||
        props.bytes_manager[3] != sizeof(c))
    {
        free(str_copy);
        prop_clear(&props);
        return 1;
    }

    else if (strcmp(props.keys[0], "Key1") != 0 ||
            strcmp(props.keys[1], "Key2") != 0 ||
            strcmp(props.keys[2], "Key3") != 0 ||
            strcmp(props.keys[3], "Key4") != 0)
    {
        free(str_copy);
        prop_clear(&props);
        return 1;
    }

    memcpy(str_copy, props.values, 4);
    memcpy(&a_copy, (char *) props.values + 4, sizeof(a));
    memcpy(&b_copy, (char *) props.values + 4 + sizeof(a), sizeof(b));
    memcpy(&c_copy, (char *) props.values + 4 + sizeof(a) + sizeof(b), sizeof(c));

    if (strcmp(str_copy, "Test") != 0 ||
            a != a_copy || b != b_copy || c != c_copy)
    {
        free(str_copy);
        prop_clear(&props);
        return 1;
    }

    free(str_copy);
    prop_clear(&props);
    return 0;
}

int test_get(void)
{
    struct properties props;
    int a = 1, b = 2;
    double c = 1.1;
    prop_insert(&props, "Key1", (void *) "Test", 4);
    prop_insert(&props, "Key2", (void *) &a, sizeof(a));
    prop_insert(&props, "Key3", (void *) &b, sizeof(b));
    prop_insert(&props, "Key4", (void *) &c, sizeof(c));

    void *a_copy = malloc(sizeof(int)), *b_copy = malloc(sizeof(int)), *c_copy = malloc(sizeof(double));
    char *str_copy = (char *) malloc(4);

    if (a_copy == NULL || b_copy == NULL || c_copy == NULL || str_copy == NULL)
    {
        prop_clear(&props);
        return 1;
    }

    if (!prop_get(props, "Key1", str_copy) ||
        !prop_get(props, "Key2", a_copy) ||
        !prop_get(props, "Key3", b_copy) ||
        !prop_get(props, "Key4", c_copy))
    {
        prop_clear(&props);
        return 1;
    }

    int a_copy_val = *((int *) a_copy), b_copy_val = *((int *) b_copy);
    double c_copy_val = *((double *) c_copy);

    if (strcmp(str_copy, "Test") != 0 ||
        a_copy_val != a || b_copy_val != b || c_copy_val != c)
    {
        prop_clear(&props);
        return 1;
    }

    prop_clear(&props);
    return 0;
}

int test_remove(void)
{
    struct properties props;
    int a = 1, b = 2;
    double c = 1.1, c_copy;
    prop_insert(&props, "Key1", (void *) "Test", 4);
    prop_insert(&props, "Key2", (void *) &a, sizeof(a));
    prop_insert(&props, "Key3", (void *) &b, sizeof(b));
    prop_insert(&props, "Key4", (void *) &c, sizeof(c));

    int8_t ret = prop_remove(&props, "Key3");

    if (!ret || props.count != 3)
    {
        prop_clear(&props);
        return 1;
    }

    if (strcmp(props.keys[2], "Key4") != 0 || props.bytes_manager[2] != sizeof(c))
    {
        return 1;
    }

    memcpy(&c_copy, (int8_t *) props.values + 4 + sizeof(a), sizeof(c));
    prop_clear(&props);
    return c == c_copy ? 0 : 1;
}

int main(void)
{
    return test_remove();
}

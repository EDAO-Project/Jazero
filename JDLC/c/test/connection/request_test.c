#include <connection/request.h>
#include <structures/property.h>
#include <string.h>

int test_init(void)
{
    struct properties props = prop_init();
    int a = 1, b = 2;
    prop_insert(&props, "Key1", &a, sizeof(a));
    prop_insert(&props, "Key2", &b, sizeof(b));

    struct request req = make_request(POST, props, "Test body");
    int a_copy, b_copy;
    prop_get(props, "Key1", &a_copy);
    prop_get(props, "Key2", &b_copy);

    if (req.operation != POST)
    {
        prop_clear(&props);
        return 1;
    }

    else if (strcmp(req.body, "Test body") != 0)
    {
        prop_clear(&props);
        return 1;
    }

    else if (a != a_copy || b != b_copy)
    {
        prop_clear(&props);
        return 1;
    }

    prop_clear(&props);
    return 0;
}

int main(void)
{
    return test_init();
}

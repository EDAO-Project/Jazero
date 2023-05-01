#include <connection/request.h>
#include <structures/property.h>

int test_init(void)
{
    struct properties props;
    int a = 1, b = 2;
    prop_insert(&props, "Key1", &a, sizeof(a));
    prop_insert(&props, "Key2", &b, sizeof(b));

    struct request req = make_request(SEARCH, props);
    int a_copy, b_copy;
    prop_get(props, "Key1", &a_copy);
    prop_get(props, "Key2", &b_copy);

    if (req.op != SEARCH)
    {
        return 1;
    }

    else if (a != a_copy || b != b_copy)
    {
        return 1;
    }

    return 0;
}

int main(void)
{
    struct properties props;
    int a = 1;
    prop_insert(&props, "Key", &a, sizeof(a));

    struct request req = make_request(SEARCH, props);
    struct address addr = init_addr("http://localhost", 65211, "/test");
    request_perform(req, addr);
    return test_init();
}

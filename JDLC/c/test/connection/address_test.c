#include <string.h>
#include <connection/address.h>

struct address create(void)
{
    return init_addr("http://localhost", 8080, "/some/path/");
}

int test_host(void)
{
    struct address addr = create();
    return strcmp(addr.host, "http://localhost");
}

int test_port(void)
{
    struct address addr = create();
    return addr.port == 8080 ? 0 : 1;
}

int test_path(void)
{
    struct address addr = create();
    return strcmp(addr.path, "/some/path/");
}

int main(void)
{
    return test_host() + test_port() + test_path();
}

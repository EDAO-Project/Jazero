#include <connection/address.h>

#include <string.h>
#include <stdlib.h>

struct address init_addr(const char *host, uint32_t port, const char *path)
{
    struct address addr = {.host = (char *) malloc(strlen(host) * sizeof(char) + 1),
                            .port = port,
                            .path = (char *) malloc(strlen(path) * sizeof(char) + 1)};

    strcpy(addr.host, host);
    strcpy(addr.path, path);
    return addr;
}

void addr_clear(struct address addr)
{
    free(addr.host);
    free(addr.path);
}

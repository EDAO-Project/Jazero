#ifndef ADDRESS_H
#define ADDRESS_H

#include <inttypes.h>

struct address
{
    char *host;
    uint32_t port;
    char *path;
};

struct address init_addr(const char *host, uint32_t port, const char *path);

#endif

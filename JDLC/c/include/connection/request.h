#ifndef REQUEST_H
#define REQUEST_H

#include <connection/address.h>
#include <structures/property.h>
#include <stdlib.h>

enum Op {GET, POST};

struct request
{
    enum Op op;
    struct properties props;
    const char *body;
};

struct response
{
    char *msg;
    size_t length;
    int32_t status;
};

struct request make_request(enum Op op, struct properties props, const char *restrict body);
struct response request_perform(struct request req, struct address addr);

#endif

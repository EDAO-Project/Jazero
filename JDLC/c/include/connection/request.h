#ifndef REQUEST_H
#define REQUEST_H

#include <connection/address.h>
#include <structures/property.h>
#include <stdlib.h>

enum op {GET, POST};

struct request
{
    enum op operation;
    struct properties props;
    const char *body;
};

struct request_response
{
    char *msg;
    size_t length;
    int32_t status;
};

struct request make_request(enum op operation, struct properties props, const char *restrict body);
struct request_response request_perform(struct request req, struct address addr);

#endif

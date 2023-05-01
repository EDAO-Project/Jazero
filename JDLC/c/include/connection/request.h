#ifndef REQUEST_H
#define REQUEST_H

#include <connection/address.h>
#include <structures/property.h>

enum Op {LOAD_EMBEDDINGS, INDEX, SEARCH};

struct request
{
    enum Op op;
    struct properties props;
};

struct response
{
    char *msg;
    int32_t status;
};

struct request make_request(enum Op op, struct properties props);
struct response request_perform(struct request req, struct address addr);

#endif

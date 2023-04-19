#ifndef JDLC_H
#define JDLC_H

#include <structures/property.h>
#include <connection/address.h>

enum operation
{
    INSERT_EMBEDDINGS,
    LOAD,
    SEARCH
};

enum response_status
{
    OK,
    JAZERO_ERROR,
    REQUEST_ERROR
};

typedef struct
{
    enum operation op;
    struct properties options;
    struct address addr;

} jdlc;

typedef struct
{
    char *msg;
    enum response_status status;
} response;

#ifdef __cplusplus
extern "C"
{
#endif

struct properties init_params_insert_embeddings(void);
struct properties init_params_load(void);
struct properties init_params_search(void);
jdlc init(enum operation op, struct properties props, struct address addr);
response perform(jdlc);

#ifdef __cplusplus
}
#endif

#endif

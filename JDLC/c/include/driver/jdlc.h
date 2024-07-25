#ifndef JDLC_H
#define JDLC_H

#include <structures/property.h>
#include <structures/query.h>
#include <structures/user.h>
#include <connection/address.h>

enum operation
{
    INSERT_EMBEDDINGS,
    LOAD,
    SEARCH,
    PING,
    CLEAR,
    CLEAR_EMBEDDINGS,
    ADD_USER,
    REMOVE_USER
};

enum response_status
{
    OK = 0,
    JAZERO_ERROR,
    REQUEST_ERROR
};

enum cosine_function
{
    NORM_COS,
    ABS_COS,
    ANG_COS
};

const char *c2str(enum cosine_function function);

enum similarity_measure
{
    COSINE,
    EUCLIDEAN
};

const char *s2str(enum similarity_measure sim);

enum entity_similarity
{
    TYPE,
    PREDICATE,
    EMBEDDING
};

const char *e2str(enum entity_similarity sim);

enum prefilter
{
    HNSW,
    NONE
};

const char *p2str(enum prefilter filter);

typedef struct
{
    enum operation op;
    struct properties options;
    struct address addr;
    char *body;
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
struct properties init_params_load(const char *storage_type, uint16_t signature_size, uint16_t band_size);
struct properties init_params_search(void);
const char *add_user_body(char *buffer, user new_user);
const char *remove_user_body(char *buffer, const char *username);
const char *load_embeddings_body(char *buffer, const char *file, const char *delimiter);
const char *load_body(char *buffer, const char *table_dir, const char *table_entity_prefix, const char *kg_prefix, uint8_t progressive);
const char *search_body(char *buffer, uint32_t top_k, enum entity_similarity entity_sim, enum cosine_function function,
        enum similarity_measure sim, enum prefilter prefilter, int query_time, query q);
uint8_t init(jdlc *restrict conn, enum operation op, struct address addr, struct properties headers, const char *body);
response perform(jdlc conn);

#ifdef __cplusplus
}
#endif

#endif

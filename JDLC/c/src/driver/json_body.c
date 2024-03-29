#include "driver/jdlc.h"
#include <stdio.h>

const char *load_embeddings_body(char *buffer, const char *file, const char *delimiter)
{
    sprintf(buffer, "{\"file\": \"%s\", \"delimiter\": \"%s\"}",
            file, delimiter);
    return buffer;
}

const char *load_body(char *buffer, const char *table_dir, const char *table_entity_prefix, const char *kg_prefix)
{
    sprintf(buffer, "{\"directory\": \"%s\", \"table-prefix\": \"%s\", \"kg-prefix\": \"%s\", \"username\": \"%s\", \"password\": \"%s\"}",
            table_dir, table_entity_prefix, kg_prefix);
    return buffer;
}

const char *search_body(char *buffer, uint32_t top_k, enum entity_similarity entity_sim, enum cosine_function function,
                        enum similarity_measure sim, enum prefilter lsh_prefilter, query q)
{
    const char *cos_func_str = c2str(function), *sim_str = s2str(sim), *prefilter_str = p2str(lsh_prefilter),
                                *entity_sim_str = e2str(entity_sim);
    const char *query_str = q2str(q);
    sprintf(buffer, "{\"top-k\": \"%d\", \"entity-similarity\": \"%s\", \"cosine-function\": \"%s\", "
                  "\"single-column-per-query-entity\": \"true\", \"weighted-jaccard\": \"false\", "
                  "\"use-max-similarity-per-column\": \"true\", \"similarity-measure\": \"%s\", \"lsh\": \"%s\", "
                  "\"query\": \"%s\"}",
                  top_k, entity_sim_str, cos_func_str, sim_str, prefilter_str, query_str);
    return buffer;
}

const char *add_user_body(char *buffer, user new_user)
{
    sprintf(buffer, "{\"new-username\": \%s\", \"new-password\": \%s\"}", new_user.username, new_user.password);
    return buffer;
}

const char *remove_user_body(char *buffer, const char *username)
{
    sprintf(buffer, "{\"old-username\": \"%s\"}", username);
    return buffer;
}

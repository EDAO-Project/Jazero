// The caller of these functions must free the memory!

#include <jdlc.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

const char *load_embeddings_body(const char *file, const char *delimiter)
{
    char *body = (char *) malloc(strlen(file) + strlen(delimiter) + 25);

    if (body == NULL)
    {
        return NULL;
    }

    sprintf(body, "{\"file\": \"%s\", \"delimiter\": \"%s\"}", file, delimiter);
    return body;
}

const char *load_body(const char *table_dir, const char *table_entity_prefix, const char *kg_prefix)
{
    size_t size = strlen(table_dir) + strlen(table_entity_prefix) + strlen(kg_prefix) + 100;
    char *body = (char *) malloc(size);

    if (body == NULL)
    {
        return NULL;
    }

    sprintf(body, "{\"directory\": \"%s\", \"table-prefix\": \"%s\", \"kg-prefix\": \"%s\"}", table_dir, table_entity_prefix, kg_prefix);
    return body;
}

/*const char *search_body(uint32_t top_k, uint8_t use_embeddings, enum cosine_function function,
                        enum similarity_measure sim, enum prefilter lsh_prefilter, query q)
{
    const char *cos_func_str = c2str(function), *sim_str = s2str(sim), *prefilter_str = p2str(lsh_prefilter);
    const char *query_str = q2str(q);
    char *body = (char *) malloc(sizeof(char) * (strlen(cos_func_str) + strlen(sim_str) + strlen(prefilter_str) +
            strlen(query_str) + 100));

    if (body == NULL)
    {
        return NULL;
    }

    sprintf(body, "{\"top-k\": \"%d\", \"use_embeddings\": \"%s\", \"cosine-function\": \"%s\", "
                  "\"single-column-per-query-entity\": \"true\", \"weighted-jaccard\": \"false\", "
                  "\"use-max-similarity-per-column\": \"true\", \"similarity-measure\": \"%s\", \"lsh\": \"%s\", "
                  "\"query\": \"%s\"}",
                  top_k, use_embeddings ? "true" : "false", cos_func_str, sim_str, prefilter_str, query_str);
    return body;
}*/

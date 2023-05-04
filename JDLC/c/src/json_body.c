#include <jdlc.h>
#include <stdio.h>

const char *load_embeddings_body(char *buffer, const char *file, const char *delimiter)
{
    sprintf(buffer, "{\"file\": \"%s\", \"delimiter\": \"%s\"}", file, delimiter);
    return buffer;
}

const char *load_body(char *buffer, const char *table_dir, const char *table_entity_prefix, const char *kg_prefix)
{
    sprintf(buffer, "{\"directory\": \"%s\", \"table-prefix\": \"%s\", \"kg-prefix\": \"%s\"}", table_dir, table_entity_prefix, kg_prefix);
    return buffer;
}

const char *search_body(char *buffer, uint32_t top_k, uint8_t use_embeddings, enum cosine_function function,
                        enum similarity_measure sim, enum prefilter lsh_prefilter, query q)
{
    const char *cos_func_str = c2str(function), *sim_str = s2str(sim), *prefilter_str = p2str(lsh_prefilter);
    const char *query_str = q2str(q);
    sprintf(buffer, "{\"top-k\": \"%d\", \"use_embeddings\": \"%s\", \"cosine-function\": \"%s\", "
                  "\"single-column-per-query-entity\": \"true\", \"weighted-jaccard\": \"false\", "
                  "\"use-max-similarity-per-column\": \"true\", \"similarity-measure\": \"%s\", \"lsh\": \"%s\", "
                  "\"query\": \"%s\"}",
                  top_k, use_embeddings ? "true" : "false", cos_func_str, sim_str, prefilter_str, query_str);
    return buffer;
}

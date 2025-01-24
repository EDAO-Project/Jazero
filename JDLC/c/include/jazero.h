#ifndef JAZERO_H
#define JAZERO_H

#include <driver/jdlc.h>
#include <structures/user.h>

response insert_embeddings(const char *ip, user u, const char *embeddings_file, const char *delimiter,
                           const char *jazero_dir, uint8_t verbose);
response load(const char *ip, user u, const char *storage_type, const char *table_entity_prefix, const char *kg_entity_prefix,
              const char *jazero_dir, const char *table_dir, uint8_t progressive, uint8_t verbose);
response search(const char *ip, user u, query q, uint32_t top_k, enum entity_similarity entity_sim,
        enum similarity_measure sim_measure, enum cosine_function embeddings_function, enum prefilter filter_type, int query_time);
response keyword_search(const char *ip, user u, const char *query);
response ping(const char *ip, user u);
response clear(const char *ip, user u);
response clear_embeddings(const char *ip, user u);
response add_user(const char *ip, user u, user new_user);
response remove_user(const char *ip, user u, const char *old_username);
response count(const char *ip, user u, const char *uri);
response stats(const char *ip, user u);
response table_stats(const char *ip, user u, const char *table_id);

#endif

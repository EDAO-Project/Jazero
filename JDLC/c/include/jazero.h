#ifndef JAZERO_H
#define JAZERO_H

#include <driver/jdlc.h>

response insert_embeddings(const char *ip, const char *embeddings_file, const char *delimiter, const char *jazero_dir,
                                uint8_t verbose);
response load(const char *ip, const char *storage_type, const char *table_entity_prefix, const char *kg_entity_prefix,
              uint16_t signature_size, uint16_t band_size, const char *jazero_dir, const char *table_dir, uint8_t verbose);
response search(const char *ip, query q, uint32_t top_k, enum entity_similarity entity_sim, enum similarity_measure sim_measure,
        enum cosine_function embeddings_function, enum prefilter filter_type);
response ping(const char *ip);

#endif

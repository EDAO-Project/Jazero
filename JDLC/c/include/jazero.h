#ifndef JAZERO_H
#define JAZERO_H

#include <driver/jdlc.h>

response insert_embeddings(struct address host, const char *embeddings_file, const char *delimiter, const char *jazero_dir);
response load(struct address host);
response search(struct address host);

#endif

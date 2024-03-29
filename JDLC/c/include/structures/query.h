#ifndef QUERY_H
#define QUERY_H

#include <inttypes.h>

typedef struct
{
    uint32_t row_count, column_count;
    char ***rows;
} query;

const char *q2str(query q);
query make_query(const char ***rows, uint32_t row_count, uint32_t column_count);
void clear_query(query q);
query parse_query_file(const char *file_name);

#endif

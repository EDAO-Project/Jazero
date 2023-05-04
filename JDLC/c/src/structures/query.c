#include <structures/query.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static inline size_t full_size(const char ***rows, uint32_t row_count, uint32_t column_count)
{
    size_t size = 0;

    for (uint32_t i = 0; i < row_count; i++)
    {
        for (uint32_t j = 0; j < column_count; j++)
        {
            size += strlen(rows[i][j]);
        }
    }

    return size;
}

// TODO: Row elements are separated by '<>', and rows are separated by '#'
// Caller must free the string
const char *q2str(query q)
{
    size_t size = full_size((const char ***) q.rows, q.row_count, q.column_count), current = 0;
    char *str = (char *) malloc(sizeof(char) * (size + q.row_count + q.column_count));

    if (str == NULL)
    {
        return NULL;
    }

    for (uint32_t i = 0; i < q.row_count; i++)
    {
        for (uint32_t j = 0; j < q.column_count; j++)
        {
            sprintf(str + current, "%s<>", q.rows[i][j]);
            current += strlen(q.rows[i][j]) + 2;
        }

        if (i < q.row_count - 1)
        {
            current -= 2;
            str[current] = '#';
            current++;
        }
    }

    str[strlen(str) - 2] = '\0';
    return str;
}

query make_query(const char ***rows, uint32_t row_count, uint32_t column_count)
{
    query q = {.row_count = row_count, .column_count = column_count};
    size_t size = full_size(rows, row_count, column_count);
    q.rows = (char ***) malloc(sizeof(char) * size);

    if (q.rows == NULL)
    {
        // TODO: Do something
    }

    memcpy(q.rows, rows, size);
    return q;
}

void clear_query(query q)
{
    free(q.rows);
}

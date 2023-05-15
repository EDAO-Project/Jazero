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

static const char *read_file(const char *file_name)
{
    size_t allocated = 500, current = 0;
    char *content = (char *) malloc(allocated);
    int c;
    FILE *f = fopen(file_name, "r");

    if (f == NULL)
    {
        return NULL;
    }

    while ((c = fgetc(f)) != -1)
    {
        content[current] = (char) c;

        if (current++ == allocated)
        {
            allocated *= 2;
            char *copy = (char *) realloc(content, allocated);

            if (copy == NULL)
            {
                free(content);
                return NULL;
            }

            content = copy;
        }
    }

    content[current] = '\0';
    return content;
}

query parse_query_file(const char *file_name)
{
    const query error = {.rows = NULL, .row_count = 0, .column_count = 0};
    uint32_t row_count, column_count, current = 0;
    char ***rows;
    const char *content = read_file(file_name);
    char c;

    if (content == NULL)
    {
        return error;
    }

    while ((c = content[current++]) != '\0')
    {

    }

    free((char *) content);
}

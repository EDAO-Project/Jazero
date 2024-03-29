#include <structures/query.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <cJSON.h>

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
        return (query) {.rows = NULL, .row_count = 0, .column_count = 0};
    }

    memcpy(q.rows, rows, size);
    return q;
}

void clear_query(query q)
{
    for (uint32_t i = 0; i < q.row_count; i++)
    {
        for (uint32_t j = 0; j < q.column_count; j++)
        {
            free(q.rows[i][j]);
        }

        free(q.rows[i]);
    }

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

// Allocates query table, except each cell. The caller must allocate enough space in each cell.
static inline void alloc_query(cJSON *json, query *q, u_int8_t *error)
{
    cJSON *json_query = cJSON_GetObjectItem(json, "queries");
    cJSON *row;
    uint8_t final_column_count = 0;

    if (json_query == NULL || !cJSON_IsArray(json_query))
    {
        *error = 1;
        return;
    }

    cJSON_ArrayForEach(row, json_query)
    {
        if (!cJSON_IsArray(row))
        {
            *error = 1;
            return;
        }

        q->row_count++;
        cJSON *column;

        if (!final_column_count)
        {
            cJSON_ArrayForEach(column, row)
            {
                q->column_count++;
            }

            final_column_count = 1;
        }
    }

    q->rows = (char ***) malloc(sizeof(char **) * q->row_count);

    for (uint32_t i = 0; i < q->row_count; i++)
    {
        q->rows[i] = (char **) malloc(sizeof(char *) * q->column_count);
    }
}

query parse_query_file(const char *file_name)
{
    query error = {.rows = NULL, .row_count = 0, .column_count = 0}, q = {.rows = NULL, .row_count = 0, .column_count = 0};
    const char *content = read_file(file_name);
    uint32_t i = 0, j = 0;

    if (content == NULL)
    {
        return error;
    }

    cJSON *json = cJSON_Parse(content);
    uint8_t alloc_error = 0;
    alloc_query(json, &q, &alloc_error);

    if (json == NULL || alloc_error)
    {
        free((char *) content);
        clear_query(q);
        return error;
    }

    cJSON *query = cJSON_GetObjectItem(json, "queries");
    cJSON *row;

    if (query == NULL || !cJSON_IsArray(query))
    {
        cJSON_Delete(json);
        free((char *) content);
        clear_query(q);
        return error;
    }

    cJSON_ArrayForEach(row, query)
    {
        if (!cJSON_IsArray(row))
        {
            cJSON_Delete(json);
            free((char *) content);
            clear_query(q);
            return error;
        }

        cJSON *column;

        cJSON_ArrayForEach(column, row)
        {
            char *cell = cJSON_GetStringValue(column);

            if (cell == NULL)
            {
                cJSON_Delete(json);
                free((char *) content);
                clear_query(q);
                return error;
            }

            q.rows[i][j] = (char *) malloc(strlen(cell));
            strcpy(q.rows[i][j], cell);
            j++;
        }

        i++;
    }

    cJSON_Delete(json);
    free((char *) content);

    return q;
}

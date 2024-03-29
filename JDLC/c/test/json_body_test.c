#include "driver/jdlc.h"
#include <string.h>
#include <stdlib.h>

int test_load_embeddings(void)
{
    char *buffer = (char *) malloc(100);
    load_embeddings_body(buffer, "some/file.txt", " ");
    const char *expected = "{\"file\": \"some/file.txt\", \"delimiter\": \" \"}";

    if (strstr(buffer, expected) == NULL)
    {
        free(buffer);
        return 1;
    }

    free(buffer);
    return 0;
}

int test_load(void)
{
    char *buffer = (char *) malloc(250);
    load_body(buffer, "some/dir/", "www", "http://www");
    const char *expected = "{\"directory\": \"some/dir/\", \"table-prefix\": \"www\", \"kg-prefix\": \"http://www\"}";

    if (strstr(buffer, expected) == NULL)
    {
        free(buffer);
        return 1;
    }

    free(buffer);
    return 0;
}

int test_search(void)
{
    char ***rows = (char ***) malloc(sizeof(char **) * 2);
    rows[0] = (char **) malloc(sizeof(char *) * 3);
    rows[1] = (char **) malloc(sizeof(char *) * 3);
    rows[0][0] = (char *) malloc(4);
    rows[0][1] = (char *) malloc(4);
    rows[0][2] = (char *) malloc(4);
    rows[1][0] = (char *) malloc(4);
    rows[1][1] = (char *) malloc(4);
    rows[1][2] = (char *) malloc(4);
    strcpy(rows[0][0], "e11");
    strcpy(rows[0][1], "e12");
    strcpy(rows[0][2], "e13");
    strcpy(rows[1][0], "e21");
    strcpy(rows[1][1], "e22");
    strcpy(rows[1][2], "e23");

    query q = make_query((const char ***) rows, 2, 3);
    char *buffer = (char *) malloc(1000);
    search_body(buffer, 10, 1, NORM_COS, EUCLIDEAN, EMBEDDINGS, q);
    const char *expected = "{\"top-k\": \"10\", \"use_embeddings\": \"true\", \"cosine-function\": \"COSINE_NORM\", "
                           "\"single-column-per-query-entity\": \"true\", \"weighted-jaccard\": \"false\", "
                           "\"use-max-similarity-per-column\": \"true\", \"similarity-measure\": \"EUCLIDEAN\", "
                           "\"lsh\": \"EMBEDDINGS\", \"query\": \"e11<>e12<>e13#e21<>e22<>e23\"}";

    if (strstr(buffer, expected) == NULL)
    {
        free(buffer);
        clear_query(q);
        return 1;
    }

    free(buffer);
    clear_query(q);
    return 0;
}

int main(void)
{
    return test_load() + test_load_embeddings() + test_search();
}

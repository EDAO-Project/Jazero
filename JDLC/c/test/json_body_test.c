#include <jdlc.h>
#include <string.h>
#include <stdlib.h>

int test_load_embeddings(void)
{
    const char *body = load_embeddings_body("some/file.txt", " ");
    const char *expected = "{\"file\": \"some/file.txt\", \"delimiter\": \" \"}";

    if (strstr(body, expected) == NULL)
    {
        free((char *) body);
        return 1;
    }

    free((char *) body);
    return 0;
}

int test_load(void)
{
    const char *body = load_body("some/dir/", "www", "http://www");
    const char *expected = "{\"directory\": \"some/dir/\", \"table-prefix\": \"www\", \"kg-prefix\": \"http://www\"}";

    //if (strcmp(body, expected) == 0)
    if (strstr(body, expected) == NULL)
    {
        free((char *) body);
        return 1;
    }

    free((char *) body);
    return 0;
}

int test_search(void)
{
    return 0;
}

int main(void)
{
    return test_load() + test_load_embeddings() + test_search();
}

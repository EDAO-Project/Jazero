#include <jazero.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <libgen.h>
#include <utils/file_utils.h>

#define TABLES_MOUNT "/srv/storage/"
#define RELATIVE_TABLES ".tables"

static uint8_t prepare_embeddings(const char *embeddings_file, const char *jazero_dir)
{
    char *mount = (char *) malloc(strlen(jazero_dir) + strlen(RELATIVE_TABLES) + 5);

    if (mount == NULL)
    {
        return 0;
    }

    sprintf(mount, "%s/%s", jazero_dir, RELATIVE_TABLES);
    return copy_file(embeddings_file, mount);
}

response insert_embeddings(struct address host, const char *embeddings_file, const char *delimiter, const char *jazero_dir)
{
    if (!prepare_embeddings(embeddings_file, jazero_dir))
    {
        return (response) {.status = JAZERO_ERROR, .msg = "Could not prepare embeddings file: File not copied to mount"};
    }

    jdlc request;
    struct properties headers = init_params_insert_embeddings();
    char *body = (char *) malloc(100 + strlen(embeddings_file) + strlen(delimiter)),
        *mount_file = (char *) malloc(strlen(TABLES_MOUNT) + strlen(embeddings_file)),
        *file_name = basename((char *) embeddings_file);

    if (body == NULL || mount_file)
    {
        free(mount_file);
        free(body);
        return (response) {.status = JAZERO_ERROR, .msg = "Ran out of memory"};
    }

    strcpy(mount_file, TABLES_MOUNT);
    strcpy(mount_file + strlen(TABLES_MOUNT), file_name);
    load_embeddings_body(body, mount_file, delimiter);

    if (!init(&request, INSERT_EMBEDDINGS, host, headers, body))
    {
        free(mount_file);
        free(body);
        return (response) {.status = JAZERO_ERROR, .msg = "Could not initialize Jazero request"};
    }

    response res = perform(request);
    free(mount_file);
    free(body);

    return res;
}

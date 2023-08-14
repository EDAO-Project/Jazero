#include "jazero.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <libgen.h>
#include <dirent.h>
#include "utils/file_utils.h"

#define TABLES_MOUNT "/srv/storage/"
#define RELATIVE_TABLES ".tables"

const uint16_t DL_PORT = 8081;
const uint16_t ENTITY_LINKER_PORT = 8082;
const uint16_t EKG_PORT = 8083;

static inline void print(uint8_t verbose, const char *message)
{
    if (verbose)
    {
        printf("%s", message);
    }
}

response ping(const char *ip)
{
    struct address dl_addr = init_addr(ip, DL_PORT, "/ping"),
            linker_addr = init_addr(ip, ENTITY_LINKER_PORT, "/ping"),
            ekg_addr = init_addr(ip, EKG_PORT, "/ping");
    jdlc dl_req, linker_req, ekg_req;
    struct properties headers = prop_init();
    uint8_t init_dl = init(&dl_req, PING, dl_addr, headers, NULL),
            init_el = init(&linker_req, PING, linker_addr, headers, NULL),
            init_ekg = init(&ekg_req, PING, ekg_addr, headers, NULL);

    if (!init_dl || !init_el || !init_ekg)
    {
        prop_clear(&headers);
        addr_clear(dl_addr);
        addr_clear(linker_addr);
        addr_clear(ekg_addr);
        return (response) {.status = JAZERO_ERROR, .msg = "Could not initialize Jazero PING request"};
    }

    response dl_response = perform(dl_req),
            el_response = perform(linker_req),
            ekg_response = perform(ekg_req);
    prop_clear(&headers);
    addr_clear(dl_addr);
    addr_clear(linker_addr);
    addr_clear(ekg_addr);

    if (dl_response.status != OK)
    {
        return dl_response;
    }

    else if (el_response.status != OK)
    {
        return el_response;
    }

    else if (ekg_response.status != OK)
    {
        return ekg_response;
    }

    return (response) {.status = OK, .msg = "Pong"};
}

response clear(const char *ip)
{
    struct address dl_addr = init_addr(ip, DL_PORT, "/clear");
    jdlc dl_req;
    struct properties headers = prop_init();
    uint8_t init_dl = init(&dl_req, CLEAR, dl_addr, headers, NULL);

    if (!init_dl)
    {
        prop_clear(&headers);
        addr_clear(dl_addr);
        return (response) {.status = JAZERO_ERROR, .msg = "Could not initialize Jazero CLEAR request"};
    }

    response dl_response = perform(dl_req);
    prop_clear(&headers);
    addr_clear(dl_addr);

    return dl_response;
}

response clear_embeddings(const char *ip)
{
    struct address dl_addr = init_addr(ip, DL_PORT, "/clear-embeddings");
    jdlc dl_req;
    struct properties headers = prop_init();
    uint8_t init_dl = init(&dl_req, CLEAR, dl_addr, headers, NULL);

    if (!init_dl)
    {
        prop_clear(&headers);
        addr_clear(dl_addr);
        return (response) {.status = JAZERO_ERROR, .msg = "Could not initialize Jazero CLEAR-EMBEDDINGS request"};
    }

    response dl_response = perform(dl_req);
    prop_clear(&headers);
    addr_clear(dl_addr);

    return dl_response;
}

static inline uint8_t prepare_embeddings(const char *embeddings_file, const char *jazero_dir)
{
    char *mount = (char *) malloc(strlen(jazero_dir) + strlen(RELATIVE_TABLES) + 5);

    if (mount == NULL)
    {
        return 0;
    }

    sprintf(mount, "%s/%s", jazero_dir, RELATIVE_TABLES);

    uint8_t ret = copy_file(embeddings_file, mount);
    free(mount);
    return ret;
}

static inline uint8_t remove_embeddings_file(const char *embeddings_file, const char *jazero_dir)
{
    char *path = (char *) malloc(strlen(jazero_dir) + strlen(RELATIVE_TABLES) + strlen(embeddings_file) + 5);

    if (path == NULL)
    {
        return 0;
    }

    sprintf(path, "%s/%s/%s", jazero_dir, RELATIVE_TABLES, embeddings_file);

    uint8_t ret = remove_file(path);
    free(path);
    return ret;
}

response insert_embeddings(const char *ip, const char *embeddings_file, const char *delimiter, const char *jazero_dir,
                                uint8_t verbose)
{
    print(verbose, "Copying embeddings file...\n");

    if (!prepare_embeddings(embeddings_file, jazero_dir))
    {
        return (response) {.status = JAZERO_ERROR, .msg = "Could not prepare embeddings file: File not copied to mount"};
    }

    print(verbose, "Copy done\n");

    jdlc request;
    struct address addr = init_addr(ip, DL_PORT, "/embeddings");
    struct properties headers = init_params_insert_embeddings();
    char *body = (char *) malloc(100 + strlen(embeddings_file) + strlen(delimiter)),
        *mount_file = (char *) malloc(strlen(TABLES_MOUNT) + strlen(embeddings_file) + 1),
        *file_name = basename((char *) embeddings_file);

    if (body == NULL || mount_file == NULL)
    {
        free(mount_file);
        free(body);
        prop_clear(&headers);
        addr_clear(addr);
        return (response) {.status = JAZERO_ERROR, .msg = "Ran out of memory"};
    }

    strcpy(mount_file, TABLES_MOUNT);
    strcpy(mount_file + strlen(TABLES_MOUNT), file_name);
    load_embeddings_body(body, mount_file, delimiter);

    if (!init(&request, INSERT_EMBEDDINGS, addr, headers, body))
    {
        free(mount_file);
        free(body);
        prop_clear(&headers);
        addr_clear(addr);
        return (response) {.status = JAZERO_ERROR, .msg = "Could not initialize Jazero INSERT_EMBEDDINGS request"};
    }

    print(verbose, "Loading embeddings...\n");

    response res = perform(request);
    free(mount_file);
    free(body);
    addr_clear(addr);
    remove_embeddings_file(embeddings_file, jazero_dir);
    print(verbose, "Loading complete\n");

    return res;
}

static inline uint8_t prepare_tables(const char *jazero_table_dir, const char *table_folder)
{
    uint8_t has_postfix = table_folder[strlen(table_folder) - 1] == '/';
    DIR *dirp = opendir(table_folder);
    struct dirent *entry;
    char *abs_path = (char *) malloc(strlen(table_folder) + 100);

    if (abs_path == NULL)
    {
        return 0;
    }

    while ((entry = readdir(dirp)) != NULL)
    {
        if (entry->d_type == DT_REG)
        {
            sprintf(abs_path, "%s%s%s", table_folder, has_postfix ? "" : "/", entry->d_name);
            copy_file(abs_path, jazero_table_dir);
        }
    }

    return 1;
}

response load(const char *ip, const char *storage_type, const char *table_entity_prefix, const char *kg_entity_prefix,
              uint16_t signature_size, uint16_t band_size, const char *jazero_dir, const char *table_dir, uint8_t verbose)
{
    char *table_storage = (char *) malloc(strlen(jazero_dir) + strlen(RELATIVE_TABLES) + 5);
    response mem_error = {.status = JAZERO_ERROR, .msg = "Ran out of memory"};

    if (table_storage == NULL)
    {
        return mem_error;
    }

    strcpy(table_storage, jazero_dir);

    if (jazero_dir[strlen(jazero_dir) - 1] == '/')
    {
        strcpy(table_storage + strlen(jazero_dir), RELATIVE_TABLES);
    }

    else
    {
        strcpy(table_storage + strlen(jazero_dir), "/");
        strcpy(table_storage + strlen(jazero_dir) + 1, RELATIVE_TABLES);
    }

    if (file_count(table_storage) > 0)
    {
        free(table_storage);
        return (response) {.status = REQUEST_ERROR, .msg = "There are already tables stored in '"RELATIVE_TABLES"'"};
    }

    print(verbose, "Copying tables...\n");

    if (!prepare_tables(table_storage, table_dir))
    {
        free(table_storage);
        return (response) {.status = JAZERO_ERROR, .msg = "Could not prepare table files: Files not copied to mount"};
    }

    print(verbose, "Copy done\n");

    struct properties headers = init_params_load(storage_type, signature_size, band_size);
    jdlc request;
    struct address addr = init_addr(ip, DL_PORT, "/insert");
    char *body = (char *) malloc(100 + strlen(TABLES_MOUNT) + strlen(table_entity_prefix) + strlen(kg_entity_prefix));

    if (body == NULL)
    {
        free(table_storage);
        prop_clear(&headers);
        addr_clear(addr);
        return mem_error;
    }

    load_body(body, TABLES_MOUNT, table_entity_prefix, kg_entity_prefix);

    if (!init(&request, LOAD, addr, headers, body))
    {
        free(table_storage);
        free(body);
        prop_clear(&headers);
        addr_clear(addr);
        return (response) {.status = JAZERO_ERROR, .msg = "Could not initialize Jazero INSERT request"};
    }

    print(verbose, "Loading tables\n");

    response res = perform(request);
    free(table_storage);

    free(body);
    prop_clear(&headers);
    addr_clear(addr);
    print(verbose, "Loading complete\n");

    return res;
}

response search(const char *ip, query q, uint32_t top_k, enum entity_similarity entity_sim, enum similarity_measure sim_measure,
                enum cosine_function embeddings_function, enum prefilter filter_type)
{
    jdlc request;
    struct properties headers = init_params_search();
    struct address addr = init_addr(ip, DL_PORT, "/search");
    char *body = (char *) malloc(1000);

    if (body == NULL)
    {
        addr_clear(addr);
        prop_clear(&headers);
        return (response) {.status = JAZERO_ERROR, .msg = "Ran out of memory"};
    }

    search_body(body, top_k, entity_sim, embeddings_function, sim_measure, filter_type, q);

    char *body_copy = (char *) realloc(body, strlen(body));

    if (body_copy != NULL)
    {
        body = body_copy;
    }

    if (!init(&request, SEARCH, addr, headers, body))
    {
        free(body);
        addr_clear(addr);
        prop_clear(&headers);
        return (response) {.status = JAZERO_ERROR, .msg = "Could not initialize Jazero SEARCH request"};
    }

    response res = perform(request);
    free(body);
    addr_clear(addr);
    prop_clear(&headers);

    return res;
}

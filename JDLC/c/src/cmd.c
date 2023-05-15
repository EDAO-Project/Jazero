#include <jazero.h>
#include <utils/file_utils.h>
#include <stdio.h>
#include <stdlib.h>
#include <argp.h>
#include <string.h>

#define DESC "Jazero C/C++ terminal tool."
#define ARG_DOC ""

struct arguments
{
    char *host, *query_file, *table_loc, *jazero_dir, *storage_type, *table_prefix, *kg_prefix, *embeddings_file,
            *delimiter, *error_msg;
    enum operation op;
    enum cosine_function cos_func;
    enum similarity_measure sim_measure;
    enum prefilter filter;
    int use_embeddings, top_k, signature_size, band_size, parse_error;
};

static struct argp_option options[] = {
        {"host", 'h', "address", 0, "Host of machine on which Jazero is deployed", 0},
        {"operation", 'o', "Jazero operation", 0, "Jazero operation to perform (search, insert, loadembeddings, ping)", 0},
        {"query", 'q', "Table query",  0, "Query file path", 0},
        {"scoringtype", 's', "Scoring function", 0, "Type of entity scoring (\'TYPE\', \'COSINE_NORM\', \'COSINE_ABS\', \'COSINE_ANG\')", 0},
        {"topk", 'k', "Top-K", 0, "Top-K value", 0},
        {"similaritymeasure", 'm', "Similarity function", 0, "Similarity measure between vectors of entity scores (\'EUCLIDEAN\', \'COSINE\')", 0},
        {"location", 'l', "Corpus location", 0, "Absolute path to table corpus directory on machine running Jazero", 0},
        {"jazerodir", 'j', "Jazero directory", 0, "Absolute path to Jazero directory on the machine running Jazero", 0},
        {"storagetype", 't', "Table storage", 0, "Type of storage for inserted table corpus (\'NATIVE\', \'HDFS\' (recommended))", 0},
        {"tableentityprefix", 'p', "Table entity prefix", 0, "Prefix of table entity URIs", 0},
        {"kgentityprefix", 'i', "KG IRI prefix", 0, "Prefix of KG entity IRIs", 0},
        {"embeddings", 'e', "Embeddings file", 0, "Absolute path to embeddings file on the machine running Jazero", 0},
        {"delimiter", 'd', "Embeddings delimiter", 0, "Delimiter in embeddings file (see README)", 0},
        {"signaturesize", 'g', "LSH signature size", 0, "Size of signature or number of permutation/projection vectors", 0},
        {"bandsize", 'b', "LSH signature band size", 0, "Size of signature bands", 0},
        {"prefilter", 'f', "LSH pre-filter", 0, "Type of LSH pre-filter (\'TYPES\', \'EMBEDDINGS\')", 0}
};

static error_t parse_opt(int key, char *arg, struct argp_state *state)
{
    struct arguments *args = state->input;

    switch (key)
    {
        case 'h':
            args->host = arg;
            break;

        case 'o':
            if (strcmp(arg, "search") == 0)
            {
                args->op = SEARCH;
            }

            else if (strcmp(arg, "insert") == 0)
            {
                args->op = LOAD;
            }

            else if (strcmp(arg, "loadembeddings") == 0)
            {
                args->op = INSERT_EMBEDDINGS;
            }

            else if (strcmp(arg, "ping") == 0)
            {
                args->op = PING;
            }

            else
            {
                args->parse_error = 1;
                args->error_msg = "Could not parse passed value for 'operation'";
            }

            break;

        case 'q':
            args->query_file = arg;
            break;

        case 's':
            if (strcmp(arg, "TYPE") == 0)
            {
                args->use_embeddings = 0;
                args->cos_func = NORM_COS;
            }

            else if (strcmp(arg, "COSINE_NORM") == 0)
            {
                args->cos_func = NORM_COS;
            }

            else if (strcmp(arg, "COSINE_ABS") == 0)
            {
                args->cos_func = ABS_COS;
            }

            else if (strcmp(arg, "COSINE_ANG") == 0)
            {
                args->cos_func = ANG_COS;
            }

            else
            {
                args->parse_error = 1;
                args->error_msg = "Could not parse passed value for 'scoringtype'";
            }

            break;

        case 'k':
            args->top_k = strtol(arg, NULL, 10);
            break;

        case 'm':
            if (strcmp(arg, "EUCLIDEAN") == 0)
            {
                args->sim_measure = EUCLIDEAN;
            }

            else if (strcmp(arg, "COSINE") == 0)
            {
                args->sim_measure = COSINE;
            }

            else
            {
                args->parse_error = 1;
                args->error_msg = "Could not parse passed value for 'similaritymeasure'";
            }

            break;

        case 'l':
            args->table_loc = arg;
            break;

        case 'j':
            args->jazero_dir = arg;
            break;

        case 't':
            if (strcmp(arg, "NATIVE") != 0 && strcmp(arg, "HDFS") != 0)
            {
                args->parse_error = 1;
                args->error_msg = "Could not parse passed value for 'storagetype'";
            }

            else
            {
                args->storage_type = arg;
            }

            break;

        case 'p':
            args->table_prefix = arg;
            break;

        case 'i':
            args->kg_prefix = arg;
            break;

        case 'e':
            args->embeddings_file = arg;
            break;

        case 'd':
            args->delimiter = arg;
            break;

        case 'g':
            args->signature_size = strtol(arg, NULL, 10);
            break;

        case 'b':
            args->band_size = strtol(arg, NULL, 10);
            break;

        case 'f':
            if (strcmp(arg, "TYPES") == 0)
            {
                args->filter = TYPES;
            }

            else if (strcmp(arg, "EMBEDDINGS") == 0)
            {
                args->filter = EMBEDDINGS;
            }

            else
            {
                args->parse_error = 1;
                args->error_msg = "Could not parse passed value for 'prefilter'";
            }

            break;

        default:
            args->parse_error = 1;
            args->error_msg = "Unrecognized option";
    }

    return 0;
}

static response do_insert_embeddings(const char *ip, const char *jazero_dir, const char *embeddings_file, const char *delimiter)
{
    if (!file_exists(jazero_dir))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Jazero directory does not exist"};
    }

    else if (!file_exists(embeddings_file))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Embeddings file does not exist"};
    }

    return insert_embeddings(ip, embeddings_file, delimiter, jazero_dir);
}

static response do_load(const char *ip, const char *jazero_dir, const char *table_dir, const char *storage_type,
                    const char *table_prefix, const char *kg_prefix, int signature_size, int band_size)
{
    if (!file_exists(jazero_dir))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Jazero directory does not exist"};
    }

    else if (!file_exists(table_dir))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Table directory does not exist"};
    }

    return load(ip, storage_type, table_prefix, kg_prefix, signature_size, band_size, jazero_dir, table_dir);
}

static response do_search(const char *ip, const char *query_file, uint8_t use_embeddings, enum cosine_function cos_func, int top_k,
        enum similarity_measure measure, enum prefilter filter)
{
    if (!file_exists(query_file))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Query file does not exist"};
    }

    query q = parse_query_file(query_file);

    /*if (q.row_count < 0)
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Could not parse JSON query file"};
    }*/

    return search(ip, q, top_k, use_embeddings, measure, cos_func, filter);
}

static response do_ping(const char *ip)
{
    return ping(ip);
}

int main(int argc, char *argv[])
{
    response ret;
    struct argp arg_p = {options, parse_opt, ARG_DOC, DESC, 0, 0, 0};
    struct arguments args = {.parse_error = 0, .use_embeddings = 0, .top_k = 100, .sim_measure = EUCLIDEAN,
            .storage_type = "NATIVE", .table_prefix = "", .kg_prefix = "", .delimiter = " ",
            .signature_size = 30, .band_size = 10, .filter = NONE};
    args.error_msg = NULL;
    args.host = NULL;
    args.jazero_dir = NULL;
    args.query_file = NULL;
    args.table_loc = NULL;

    argp_parse(&arg_p, argc, argv, 0, 0, &args);

    if (args.parse_error)
    {
        printf("Error: %s\n", args.error_msg);
        return EXIT_FAILURE;
    }

    else if (args.host == NULL)
    {
        printf("Error: Missing host name\n");
        return EXIT_FAILURE;
    }

    switch (args.op)
    {
        case INSERT_EMBEDDINGS:
            if (args.jazero_dir == NULL || args.embeddings_file == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Missing either Jazero directory of embeddings file"};
                break;
            }

            ret = do_insert_embeddings(args.host, args.jazero_dir, args.embeddings_file, args.delimiter);
            break;

        case LOAD:
            if (args.jazero_dir == NULL || args.embeddings_file == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Missing either Jazero directory of embeddings file"};
                break;
            }

            else if (args.signature_size < 1 || args.band_size < 1)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Signature size of band size must be greater than 1\n"};
                break;
            }

            ret = do_load(args.host, args.jazero_dir, args.table_loc, args.storage_type, args.table_prefix,
                       args.kg_prefix, args.signature_size, args.band_size);
            break;

        case SEARCH:
            if (args.query_file == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Missing query file\n"};
                break;
            }
            ret = do_search(args.host, args.query_file, args.use_embeddings, args.cos_func, args.top_k, args.sim_measure, args.filter);
            break;

        case PING:
            ret = do_ping(args.host);
            break;

        default:
            ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Unrecognized operation (%d)\n"};
    }

    printf("%s\n", ret.msg);
    return ret.status;
}

#include <driver/jdlc.h>
#include <stdio.h>
#include <stdlib.h>
#include <argp.h>
#include <string.h>

#define EMAIL "mpch@cs.aau.dk"
#define VERSION "Jazero 1.0"
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
    }

    return 0;
}

int main(int argc, char *argv[])
{
    struct argp arg_p = {options, parse_opt, ARG_DOC, DESC, 0, 0, 0};
    struct arguments args = {.error_msg = NULL, .parse_error = 0, .use_embeddings = 0, .top_k = 100,
            .sim_measure = EUCLIDEAN, .storage_type = "NATIVE", .table_prefix = "", .kg_prefix = "", .delimiter = " ",
            .signature_size = 30, .band_size = 10, .filter = NONE};
    argp_parse(&arg_p, argc, argv, 0, 0, &args);

    if (args.parse_error)
    {
        perror(args.error_msg);
        return EXIT_FAILURE;
    }

    switch (args.op)
    {
        case INSERT_EMBEDDINGS:
            break;

        case LOAD:
            break;

        case SEARCH:
            break;

        case PING:
            break;
    }

    return EXIT_SUCCESS;
}

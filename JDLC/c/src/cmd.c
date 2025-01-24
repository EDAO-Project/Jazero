#include <jazero.h>
#include <utils/file_utils.h>
#include <stdio.h>
#include <stdlib.h>
#include <argp.h>
#include <string.h>

#define USAGE "usage: jdlc [option] ...\n" \
                "-o, --operation : Jazero operation to perform (search, keyword, insert, insertembeddings, ping, clear, clearembeddings, adduser, removeuser, count, stats, tablestats)\n" \
                "\nping, search, keyword, insert, insertembeddings, clear, clearembeddings, adduser, removeuser, count, stats, tablestats\n" \
                "-h, --host : Host of machine on which Jazero is deployed\n" \
                "-u, --username : Username of this user\n" \
                "-c, --password : Password for this user\n" \
                "\nsearch\n" \
                "-q, --query : Query file path\n" \
                "-s, --scoringtype : Type of entity scoring ('TYPE', 'PREDICATE', 'COSINE_NORM', 'COSINE_ABS', 'COSINE_ANG')\n" \
                "-k, --topk : Top-K value\n" \
                "-m, --similaritymeasure : Similarity measure between vectors of entity scores ('EUCLIDEAN', 'COSINE')\n" \
                "-f, --prefilter : Whether to perform search space pre-filtering with HNSW ('TRUE', 'FALSE'), which is by default FALSE\n" \
                "-qt, --querytime : Maximum amount of seconds allowed to be spend on indexing before query execution (optional and only used during progressive indexing)\n" \
                "\nkeyword\n" \
                "-kw, --keywords : Keyword query string to search for KG entities\n" \
                "\ninsert, insertembeddings\n" \
                "-j, --jazerodir : Absolute path to Jazero directory on the machine running Jazero\n" \
                "\ninsert\n" \
                "-l, --location : Absolute path to table corpus directory on machine running Jazero\n" \
                "-t, --storagetype : Type of storage for inserted table corpus ('NATIVE', 'HDFS' (recommended))\n" \
                "-p, --tableentityprefix : Prefix of table entity URIs\n" \
                "-i, --kgentityprefix : Prefix of KG entity IRIs\n" \
                "-prog, --progressive : Enable progressive indexing ('true', 'false')\n" \
                "\ninsertembeddings\n" \
                "-e, --embeddings : Absolute path to embeddings file on the machine running Jazero\n" \
                "-d, --delimiter : Delimiter in embeddings file (see README)\n" \
                "\nadduser\n" \
                "-nu, --newusername : Username of new user\n" \
                "-nc, --newpassword : Password of new user\n" \
                "\nremoveuser\n" \
                "-ou, --oldusername : Username of user to remove\n" \
                "\ncount\n" \
                "-n, --count : URI of entity to retrieve count of in data lake tables" \
                "\ntablestats\n" \
                "-s, --table : Table file ID to retrieve stats for" \

struct arguments
{
    char *host, *query_file, *keyword_query, *table_loc, *jazero_dir, *storage_type, *table_prefix, *kg_prefix, *embeddings_file,
            *delimiter, *error_msg, *this_username, *this_password, *new_username, *new_password, *old_username, *uri, *table_id;
    enum operation op;
    enum cosine_function cos_func;
    enum similarity_measure sim_measure;
    enum prefilter filter;
    int top_k, parse_error, query_time, progressive;
    enum entity_similarity entity_sim;
};

static response do_insert_embeddings(const char *ip, const char *username, const char *password, const char *jazero_dir,
                                     const char *embeddings_file, const char *delimiter)
{
    if (!file_exists(jazero_dir))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Jazero directory does not exist"};
    }

    else if (!file_exists(embeddings_file))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Embeddings file does not exist"};
    }

    user u = create_user(username, password);
    return insert_embeddings(ip, u, embeddings_file, delimiter, jazero_dir, 1);
}

static response do_load(const char *ip, const char *username, const char *password, const char *jazero_dir,
                        const char *table_dir, const char *storage_type, const char *table_prefix, const char *kg_prefix,
                        int progressive)
{
    if (!file_exists(jazero_dir))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Jazero directory does not exist"};
    }

    else if (!file_exists(table_dir))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Table directory does not exist"};
    }

    user u = create_user(username, password);
    return load(ip, u, storage_type, table_prefix, kg_prefix, jazero_dir, table_dir, progressive, 1);
}

static response do_search(const char *ip, const char *username, const char *password, const char *query_file,
                          enum entity_similarity entity_sim, enum cosine_function cos_func, int top_k,
                                  enum similarity_measure measure, enum prefilter filter, int query_time)
{
    if (!file_exists(query_file))
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Query file does not exist"};
    }

    query q = parse_query_file(query_file);
    user u = create_user(username, password);

    if (q.rows == NULL)
    {
        return (response) {.status = REQUEST_ERROR, .msg = "Could not parse JSON query file"};
    }

    return search(ip, u, q, top_k, entity_sim, measure, cos_func, filter, query_time);
}

static response do_keyword_search(const char *ip, const char *username, const char *password, const char *query)
{
    user u = create_user(username, password);
    return keyword_search(ip, u, query);
}

static response do_ping(const char *ip, const char *username, const char *password)
{
    user u = create_user(username, password);
    return ping(ip, u);
}

static inline uint8_t check_key(const char *key, const char *short_check, const char *long_check)
{
    return strcmp(key, short_check) == 0 || strcmp(key, long_check) == 0;
}

static response do_clear(const char *ip, const char *username, const char *password)
{
    user u = create_user(username, password);
    return clear(ip, u);
}

static response do_clear_embeddings(const char *ip, const char *username, const char *password)
{
    user u = create_user(username, password);
    return clear_embeddings(ip, u);
}

static response do_add_user(const char *ip, const char *username, const char *password, const char *new_username, const char *new_password)
{
    user u = create_user(username, password), new_user = create_user(new_username, new_password);
    return add_user(ip, u, new_user);
}

static response do_remove_user(const char *ip, const char *username, const char *password, const char *old_username)
{
    user u = create_user(username, password);
    return remove_user(ip, u, old_username);
}

static response do_count(const char *ip, const char *username, const char *password, const char *uri)
{
    user u = create_user(username, password);
    return count(ip, u, uri);
}

static response do_stats(const char *ip, const char *username, const char *password)
{
    user u = create_user(username, password);
    return stats(ip, u);
}

static response do_table_stats(const char *ip, const char *username, const char *password, const char *table_id)
{
    user u = create_user(username, password);
    return table_stats(ip, u, table_id);
}

error_t parse(const char *key, const char *arg, struct arguments *args)
{
    if (check_key(key, "-h", "--host"))
    {
        args->host = (char *) arg;
    }

    else if (check_key(key, "-o", "--operation"))
    {
        if (strcmp(arg, "search") == 0)
        {
            args->op = SEARCH;
        }

        if (strcmp(arg, "keyword") == 0)
        {
            args->op = KEYWORD;
        }

        else if (strcmp(arg, "insert") == 0)
        {
            args->op = LOAD;
        }

        else if (strcmp(arg, "insertembeddings") == 0)
        {
            args->op = INSERT_EMBEDDINGS;
        }

        else if (strcmp(arg, "ping") == 0)
        {
            args->op = PING;
        }

        else if (strcmp(arg, "clear") == 0)
        {
            args->op = CLEAR;
        }

        else if (strcmp(arg, "clearembeddings") == 0)
        {
            args->op = CLEAR_EMBEDDINGS;
        }

        else if (strcmp(arg, "adduser") == 0)
        {
            args->op = ADD_USER;
        }

        else if (strcmp(arg, "removeuser") == 0)
        {
            args->op = REMOVE_USER;
        }

        else if (strcmp(arg, "count") == 0)
        {
            args->op = COUNT;
        }

        else if (strcmp(arg, "stats") == 0)
        {
            args->op = STATS;
        }

        else if (strcmp(arg, "tablestats") == 0)
        {
            args->op = TABLE_STATS;
        }

        else
        {
            args->parse_error = 1;
            args->error_msg = "Could not parse passed value for 'operation'";
        }
    }

    else if (check_key(key, "-q", "--query"))
    {
        args->query_file = (char *) arg;
    }

    else if (check_key(key, "-kw", "--keywords"))
    {
        args->keyword_query = (char *) arg;
    }

    else if (check_key(key, "-s", "--scoringtype"))
    {
        if (strcmp(arg, "TYPE") == 0)
        {
            args->entity_sim = TYPE;
            args->cos_func = NORM_COS;
        }

        else if (strcmp(arg, "PREDICATE") == 0)
        {
            args->entity_sim = PREDICATE;
            args->cos_func = NORM_COS;
        }

        else if (strcmp(arg, "COSINE_NORM") == 0)
        {
            args->entity_sim = EMBEDDING;
            args->cos_func = NORM_COS;
        }

        else if (strcmp(arg, "COSINE_ABS") == 0)
        {
            args->entity_sim = EMBEDDING;
            args->cos_func = ABS_COS;
        }

        else if (strcmp(arg, "COSINE_ANG") == 0)
        {
            args->entity_sim = EMBEDDING;
            args->cos_func = ANG_COS;
        }

        else
        {
            args->parse_error = 1;
            args->error_msg = "Could not parse passed value for 'scoringtype'";
        }
    }

    else if (check_key(key, "-k", "--topk"))
    {
        args->top_k = strtol(arg, NULL, 10);
    }

    else if (check_key(key, "-m", "--similaritymeasure"))
    {
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
    }
    else if (check_key(key, "-qt", "--querytime"))
    {
        args->query_time = strtol(arg, NULL, 10);
    }

    else if (check_key(key, "-l", "--location"))
    {
        args->table_loc = (char *) arg;
    }

    else if (check_key(key, "-j", "--jazerodir"))
    {
        args->jazero_dir = (char *) arg;
    }

    else if (check_key(key, "-t", "--storagetype"))
    {
        if (strcmp(arg, "NATIVE") != 0 && strcmp(arg, "HDFS") != 0)
        {
            args->parse_error = 1;
            args->error_msg = "Could not parse passed value for 'storagetype'";
        }

        else
        {
            args->storage_type = (char *) arg;
        }
    }

    else if (check_key(key, "-p", "----tableentityprefix"))
    {
        args->table_prefix = (char *) arg;
    }

    else if (check_key(key, "-i", "--kgentityprefix"))
    {
        args->kg_prefix = (char *) arg;
    }

    else if (check_key(key, "-e", "--embeddings"))
    {
        args->embeddings_file = (char *) arg;
    }

    else if (check_key(key, "-d", "--delimiter"))
    {
        args->delimiter = (char *) arg;
    }

    else if (check_key(key, "-f", "--prefilter"))
    {
        if (strcmp(arg, "TRUE") == 0)
        {
            args->filter = HNSW;
        }

        else if (strcmp(arg, "FALSE") == 0)
        {
            args->filter = NONE;
        }

        else
        {
            args->parse_error = 1;
            args->error_msg = "Could not parse passed value for 'prefilter'";
        }
    }

    else if (check_key(key, "-prog", "--progressive"))
    {
        if (strcmp(arg, "true") == 0)
        {
            args->progressive = 1;
        }

        else if (strcmp(arg, "false") == 0)
        {
            args->progressive = 0;
        }

        else
        {
            args->parse_error = 1;
            args->error_msg = "Could not parse passed value for 'progressive'";
        }
    }

    else if (check_key(key, "-u", "--username"))
    {
        args->this_username = (char *) arg;
    }

    else if (check_key(key, "-c", "--password"))
    {
        args->this_password = (char *) arg;
    }

    else if (check_key(key, "-nu", "--newusername"))
    {
        args->new_username = (char *) arg;
    }

    else if (check_key(key, "-nc", "--newpassword"))
    {
        args->new_password = (char *) arg;
    }

    else if (check_key(key, "-ou", "--oldusername"))
    {
        args->old_username = (char *) arg;
    }

    else if (check_key(key, "-n", "--count"))
    {
        args->uri = (char *) arg;
    }

    else if (check_key(key, '-s', '--table'))
    {
        args->table_id = (char *) arg;
    }

    else
    {
        args->parse_error = 1;
        args->error_msg = "Unrecognized option";
        return 0;
    }

    return 1;
}

int main(int argc, char *argv[])
{
    response ret;
    struct arguments args = {.parse_error = 0, .entity_sim = TYPE, .top_k = 100, .sim_measure = EUCLIDEAN,
            .storage_type = "NATIVE", .table_prefix = "", .kg_prefix = "", .delimiter = " ", .query_time = 0,
            .filter = NONE};
    args.error_msg = NULL;
    args.host = NULL;
    args.jazero_dir = NULL;
    args.query_file = NULL;
    args.keyword_query = NULL;
    args.table_loc = NULL;
    args.uri = NULL;
    args.table_id = NULL;

    for (int arg = 1; arg < argc; arg += 2)
    {
        if (strcmp(argv[arg], "--help") == 0)
        {
            printf("%s\n", USAGE);
            return EXIT_SUCCESS;
        }

        if (!parse(argv[arg], argv[arg + 1], &args))
        {
            break;
        }
    }

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

    else if (args.this_username == NULL || args.this_password == NULL)
    {
        printf("Missing authentication: Use options '-u' and '-c' to specify your username and password\n");
        return REQUEST_ERROR;
    }

    response ping = do_ping(args.host, args.this_username, args.this_password);

    if (ping.status == JAZERO_ERROR)
    {
        printf("Jazero is experiencing problems. Check the Jazero logs for more information.\n");
        return ping.status;
    }

    else if (ping.status == REQUEST_ERROR)
    {
        printf("Request error\n");
        return ping.status;
    }

    switch (args.op)
    {
        case INSERT_EMBEDDINGS:
            if (args.jazero_dir == NULL || args.embeddings_file == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Missing either Jazero directory or embeddings file"};
                break;
            }

            ret = do_insert_embeddings(args.host, args.this_username, args.this_password, args.jazero_dir,
                                       args.embeddings_file, args.delimiter);
            break;

        case LOAD:
            if (args.jazero_dir == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Missing Jazero directory"};
                break;
            }

            ret = do_load(args.host, args.this_username, args.this_password, args.jazero_dir,
                          args.table_loc, args.storage_type, args.table_prefix, args.kg_prefix, args.progressive);
            break;

        case SEARCH:
            if (args.query_file == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Missing query file\n"};
                break;
            }

            ret = do_search(args.host, args.this_username, args.this_password, args.query_file,
                            args.entity_sim, args.cos_func, args.top_k,args.sim_measure, args.filter, args.query_time);
            break;

        case KEYWORD:
            if (args.keyword_query == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Missing keyword query\n"};
                break;
            }

            ret = do_keyword_search(args.host, args.this_username, args.this_password, args.keyword_query);
            break;

        case PING:
            ret = ping;
            break;

        case CLEAR:
            ret = do_clear(args.host, args.this_username, args.this_password);
            break;

        case CLEAR_EMBEDDINGS:
            ret = do_clear_embeddings(args.host, args.this_username, args.this_password);
            break;

        case ADD_USER:
            if (args.new_username == NULL || args.new_password == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Missing username or password of user to be added"};
                break;
            }

            ret = do_add_user(args.host, args.this_username, args.this_password, args.new_username, args.new_password);
            break;

        case REMOVE_USER:
            if (args.old_username == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Missing username of user to be removed"};
                break;
            }

            ret = do_remove_user(args.host, args.this_username, args.this_password, args.old_username);
            break;

        case COUNT:
            if (args.uri == NULL)
            {
                ret = (response) {.status = REQUEST_ERROR, .msg = "Missing URI of entity"};
                break;
            }

            ret = do_count(args.host, args.this_username, args.this_password, args.uri);
            break;

        case STATS:
            ret = do_stats(args.host, args.this_username, args.this_password);
            break;

        case TABLE_STATS:
            ret = do_table_stats(args.host, args.this_username, args.this_password, args.table_id);
            break;

        default:
            ret = (response) {.status = REQUEST_ERROR, .msg = "Error: Unrecognized operation\n"};
    }

    printf("%s\n", ret.msg);
    return ret.status;
}

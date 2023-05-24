#include <structures/query.h>
#include <string.h>
#include <stdlib.h>

int test2str(void)
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
    const char *str = q2str(q);

    if (str == NULL || strcmp(str, "e11<>e12<>e13#e21<>e22<>e23") != 0)
    {
        clear_query(q);
        free((char *) str);
        return 1;
    }

    free((char *) str);
    clear_query(q);
    return 0;
}

// I will allow the memory leak here
int test_make_query(void)
{
    char ***rows = (char ***) malloc(sizeof(char **) * 3);
    rows[0] = (char **) malloc(sizeof(char *) * 3);
    rows[1] = (char **) malloc(sizeof(char *) * 3);
    rows[2] = (char **) malloc(sizeof(char *) * 3);
    rows[0][0] = (char *) malloc(4);
    rows[0][1] = (char *) malloc(4);
    rows[0][2] = (char *) malloc(4);
    rows[1][0] = (char *) malloc(4);
    rows[1][1] = (char *) malloc(4);
    rows[1][2] = (char *) malloc(4);
    rows[2][0] = (char *) malloc(4);
    rows[2][1] = (char *) malloc(4);
    rows[2][2] = (char *) malloc(4);
    strcpy(rows[0][0], "e11");
    strcpy(rows[0][1], "e12");
    strcpy(rows[0][2], "e13");
    strcpy(rows[1][0], "e21");
    strcpy(rows[1][1], "e22");
    strcpy(rows[1][2], "e23");
    strcpy(rows[2][0], "e31");
    strcpy(rows[2][1], "e32");
    strcpy(rows[2][2], "e33");

    query q = make_query((const char ***) rows, 3, 3);

    if (q.row_count != 3 || q.column_count != 3)
    {
        clear_query(q);
        return 1;
    }

    if (strcmp(q.rows[0][0], "e11") != 0 ||
            strcmp(q.rows[0][1], "e12") != 0 ||
            strcmp(q.rows[0][2], "e13") != 0 ||
            strcmp(q.rows[1][0], "e21") != 0 ||
            strcmp(q.rows[1][1], "e22") != 0 ||
            strcmp(q.rows[1][2], "e23") != 0 ||
            strcmp(q.rows[2][0], "e31") != 0 ||
            strcmp(q.rows[2][1], "e32") != 0 ||
            strcmp(q.rows[2][2], "e33") != 0)
    {
        clear_query(q);
        return 1;
    }

    clear_query(q);
    return 0;
}

int test_parse_query(void)
{
    query q = parse_query_file("../test/structures/query.json");

    if (q.rows == NULL)
    {
        return 1;
    }

    else if (q.row_count != 1 || q.column_count != 3)
    {
        return 1;
    }

    else if (strcmp(q.rows[0][0], "http://dbpedia.org/resource/Jacob_Levitzki") != 0 ||
                strcmp(q.rows[0][1], "http://dbpedia.org/resource/Hard_and_soft_science") != 0 ||
                strcmp(q.rows[0][2], "http://dbpedia.org/resource/Alexander_Levitzki") != 0)
    {
        return 1;
    }

    return 0;
}

int test_json2str(void)
{
    query q = parse_query_file("../test/structures/query.json");

    if (q.rows == NULL)
    {
        return 1;
    }

    const char *str = q2str(q);

    if (str == NULL || strcmp(str, "http://dbpedia.org/resource/Jacob_Levitzki<>http://dbpedia.org/resource/Hard_and_soft_science<>http://dbpedia.org/resource/Alexander_Levitzki") != 0)
    {
        clear_query(q);
        free((char *) str);
        return 1;
    }

    free((char *) str);
    clear_query(q);
    return 0;
}

int main(void)
{
    return test_make_query() + test2str() + test_parse_query();
}

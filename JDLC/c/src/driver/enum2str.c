#include "driver/jdlc.h"

const char *c2str(enum cosine_function function)
{
    if (function == NORM_COS)
    {
        return "NORM_COS";
    }

    else if (function == ABS_COS)
    {
        return "ABS_COS";
    }

    return "ANG_COS";
}

const char *s2str(enum similarity_measure sim)
{
    if (sim == COSINE)
    {
        return "COSINE";
    }

    return "EUCLIDEAN";
}

const char *e2str(enum entity_similarity sim)
{
    if (sim == TYPE)
    {
        return "TYPES";
    }

    else if (sim == PREDICATE)
    {
        return "PREDICATES";
    }

    return "EMBEDDINGS";
}

const char *p2str(enum prefilter filter)
{
    if (filter == TYPES)
    {
        return "TYPES";
    }

    else if (filter == NONE)
    {
        return "";
    }

    return "EMBEDDINGS";
}

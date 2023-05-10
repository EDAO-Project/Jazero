#include "driver/jdlc.h"

const char *c2str(enum cosine_function function)
{
    if (function == NORM_COS)
    {
        return "COSINE_NORM";
    }

    else if (function == ABS_COS)
    {
        return "COSINE_ABS";
    }

    return "COSINE_ANG";
}

const char *s2str(enum similarity_measure sim)
{
    if (sim == COSINE)
    {
        return "COSINE";
    }

    return "EUCLIDEAN";
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

#include <utils/file_utils.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static inline void replace(char from, char to, char *restrict str)
{
    size_t length = strlen(str);

    for (size_t i = 0; i < length; i++)
    {
        if (str[i] == from)
        {
            str[i] = to;
        }
    }
}

static int perform_op(const char *op, const char *src, const char *dst)
{
    char *command = (char *) malloc(strlen(src) + strlen(dst) + 10);

    if (command == NULL)
    {
        return 0;
    }

#ifdef UNIX
    sprintf(command, "%s %s %s", op, src, dst);
#elif defined(WINDOWS)
    size_t src_length = strlen(src), dst_length = strlen(dst);
    char *src_copy = (char *) malloc(src_length), *dst_copy = (char *) malloc(dst_length);

    if (src_copy == NULL || dst_copy == NULL)
    {
        return 0;
    }

    strcpy(src_copy, src);
    strcpy(dst_copy, dst);
    replace('/', '\\', src_copy);
    replace('/', '\\', dst_copy);
    sprintf(command, "copy %s %s", src_copy, dst_copy);
    free(src_copy);
    free(dst_copy);
#endif

    int ret = system(command);
    free(command);

    return ret != -1;
}

uint8_t copy_file(const char *src, const char *dst)
{
#ifdef UNIX
    return perform_op("cp", src, dst);
#elif defined(WINDOWS)
    return perform_op("copy", src, dst);
#endif
}

uint8_t move_file(const char *src, const char *dst)
{
#ifdef UNIX
    return perform_op("mv", src, dst);
#elif WINDOWS
    return perform_op("move", src, dst);
#endif
}

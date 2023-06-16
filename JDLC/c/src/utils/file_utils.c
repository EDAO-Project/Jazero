#include <utils/file_utils.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifdef UNIX
#include <unistd.h>
#elif defined(WINDOWS)
#include <io.h>
#define F_OK 0
#define access _access
#endif

#ifdef WINDOWS
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
#endif

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
    return perform_op("cp -r", src, dst);
#elif defined(WINDOWS)
    return perform_op("copy", src, dst);
#endif
}

uint8_t move_file(const char *src, const char *dst)
{
#ifdef UNIX
    return perform_op("mv", src, dst);
#elif defined(WINDOWS)
    return perform_op("move", src, dst);
#endif
}

uint8_t file_exists(const char *path)
{
    return access(path, F_OK) == 0;
}

uint8_t remove_file(const char *path)
{
#ifdef UNIX
    return perform_op("rm", path, "");
#elif defined(WINDOWS)
    return perorm_op("del", path, "");
#endif
}

uint32_t file_count(const char *path)
{
#ifdef WINDOWS
    return -1;
#else
    char *command = (char *) malloc(strlen(path) + 10);

    if (command == NULL)
    {
        return -1;
    }

    sprintf(command, "ls %s | wc -l", path);

    FILE *p = popen(command, "r");
    char buffer[20];

    if (p == NULL)
    {
        return -1;
    }

    fgets(buffer, 20, p);
    fclose(p);
    return strtol(buffer, (char **) NULL, 10);
#endif
}

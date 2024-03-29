#include "structures/user.h"

user create_user(const char *username, const char *password)
{
    return (user) {.username = (char *) username, .password = (char *) password};
}

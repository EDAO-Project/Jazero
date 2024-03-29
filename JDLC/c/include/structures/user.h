#ifndef USER_H
#define USER_H

typedef struct
{
    char *username, *password;
} user;

user create_user(const char *username, const char *password);

#endif

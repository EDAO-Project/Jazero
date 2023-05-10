#include <driver/jdlc.h>
#include <connection/request.h>
#include <stdlib.h>
#include <string.h>

uint8_t init(jdlc *restrict conn, enum operation op, struct address addr, struct properties headers, const char *body)
{
    conn->op = op;
    conn->addr = addr;
    conn->options = headers;
    conn->body = NULL;

    if (body != NULL)
    {
        conn->body = (char *) malloc(strlen(body));

        if (conn->body == NULL)
        {
            return 0;
        }

        strcpy(conn->body, body);
    }

    return 1;
}

/*response perform(jdlc)
{

}*/

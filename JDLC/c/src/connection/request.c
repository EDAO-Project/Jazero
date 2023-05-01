#include <connection/request.h>
#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct request make_request(enum Op op, struct properties props)
{
    return (struct request) {.op = op, .props = props};
}

static char *get_url(struct address addr)
{
    char *url = NULL;
    char *protocol = "";
    int port_length = 6;
    size_t size = sizeof(char) * (strlen(addr.host) + strlen(addr.path) + port_length);

    if (strstr(addr.host, "http") == NULL)
    {
        size += 7;
        protocol = "http://";
    }

    url = (char *) malloc(size);

    if (url == NULL)
    {
        return NULL;
    }

    sprintf(url, "%s%s:%d%s", protocol, addr.host, addr.port, addr.path);
    return url;
}

struct response request_perform(struct request req, struct address addr)
{
    char *url = get_url(addr);
    CURL *handle = curl_easy_init();
    curl_easy_setopt(handle, CURLOPT_URL, url);

    free(url);

    return (struct response) {.msg = "Test", .status = req.op};
}

#include "util.h"

char *hashit(unsigned int vdisk, unsigned long vblock)
{
    int i;
    unsigned char *hash = new unsigned char[SHA_DIGEST_LENGTH + 1];
    char *hex_hash = new char[SHA_DIGEST_HEX_LENGTH + 1];
    unsigned long inp[] = {vdisk, vblock};
    SHA1((unsigned char *) inp, sizeof(inp), hash);
    for (i = 0; i < SHA_DIGEST_LENGTH; i++)
        sprintf(&hex_hash[i * 2], "%02x", hash[i]);
    hex_hash[SHA_DIGEST_HEX_LENGTH] = '\0';
    delete[] hash;
    return hex_hash;
}

char *int64_to_binary(unsigned long dec)
{
    unsigned long j, rem;
    char *str = new char[65];
    memset(str, '0', 65);
    j = 0;
    while (dec)
    {
        rem = dec % 2;
        str[63 - j] = rem ? '1' : '0';
        j++;
        dec /= 2;
    }
    return str;
}

void line(int n, FILE *file)
{
    for (int i = 0; i < n; i++) fputc('-', file);
    fputc('\n', file);
}

bool file_exists(const char *fname)
{
    struct stat buf;
    return stat(fname, &buf) == 0;
}
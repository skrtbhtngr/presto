#include "metadata.h"

int comp_hm1(void *a, void *b)
{
    int ret;
    char *k1 = ((key_hm1 *) a)->hash;
    char *k2 = ((key_hm1 *) b)->hash;
    if (strlen(k1) != strlen(k2))
        fprintf(stderr, "%lu\n%lu\n", strlen(k1), strlen(k2));
    ret = strcmp(k1, k2);
    if (ret > 0)
        return 1;
    else if (ret < 0)
        return -1;
    else
        return 0;
}

key_hm1 *get_hm1_key(int vdisk, long vblock, const char *hash)
{
    struct key_hm1 *khm;
    khm = new key_hm1;
    assert(khm != nullptr);
    memset(khm, 0, sizeof(key_hm1));
    strncpy(khm->hash, hash, SHA_DIGEST_HEX_LENGTH);
    khm->eid.vblock = vblock;
    khm->eid.vdisk = vdisk;
    return khm;
}

val_hm1 *get_hm1_val(int vdisk, long vblock, const char *hash)
{
    char buf[HASH_CHARS_FOR_EGROUP + 1] = {0};
    struct val_hm1 *vhm = new val_hm1;
    assert(vhm != nullptr);
    //vhm->eid.vdisk = vdisk;
    //vhm->eid.vblock = vblock;
    /* taking the 7 hash characters from 10th idx to form egroup id */
    strncpy(buf, hash + 10, HASH_CHARS_FOR_EGROUP);
    vhm->egroup_id = strtoul(buf, nullptr, 16);
    return vhm;
}

int comp_hm3(void *a, void *b)
{
    unsigned long k1 = ((key_hm3 *) a)->egroup_id;
    unsigned long k2 = ((key_hm3 *) b)->egroup_id;
    if (k1 > k2)
        return 1;
    else if (k1 < k2)
        return -1;
    else
        return 0;
}

key_hm3 *get_hm3_key(long egroup_id)
{
    struct key_hm3 *khm;
    khm = new key_hm3;
    assert(khm != nullptr);
    khm->egroup_id = egroup_id;
    return khm;
}

val_hm3 *get_hm3_val(long egroup_id, int vdisk, long vblock,
                     long vblock_offset, long size)
{
    struct val_hm3 *vhm = new val_hm3;
    assert(vhm != nullptr);
    memset(vhm, 0, sizeof(val_hm3));
    vhm->extents[vhm->num_extents].eid.vdisk = vdisk;
    vhm->extents[vhm->num_extents].eid.vblock = vblock;
    vhm->num_extents++;
    return vhm;
}

void update_hm3_val(val_hm3 *vhm, int egroup_id, int vdisk,
                    long vblock, long vblock_offset, long size)
{

    vhm->extents[vhm->num_extents].eid.vdisk = vdisk;
    vhm->extents[vhm->num_extents].eid.vblock = vblock;
    vhm->num_extents++;
    if (vhm->num_extents > NUM_EXTENTS_IN_EGROUP)
        fprintf(stderr, "Overflow in egroup!\n");
}

int comp_bitset(void *a, void *b)
{
    unsigned long k1 = *(unsigned long *) a;
    unsigned long k2 = *(unsigned long *) b;
    if (k1 > k2)
        return 1;
    else if (k1 < k2)
        return -1;
    else
        return 0;
}

//void print_bitset(void *data, void *fp)
//{
//    auto *p = (Pair *) data;
//    unsigned long key = *(unsigned long *) p->key;
//    unsigned long val = *(unsigned long *) p->val;
//    unsigned long tmp, rem, i;
//    int c;
//    char str[64];
//
//    c = bitcount(val);
//    if (c < 2)
//        return;
//    memset(str, '0', 64);
//    fprintf((FILE *) fp, "| %6lu | ", key);
//    tmp = val;
//    i = 0;
//    while (tmp)
//    {
//        rem = tmp % 2;
//        str[63 - i] = rem ? '1' : '0';
//        tmp /= 2;
//        i++;
//    }
//    for (i = 0; i < 64; i++)
//        fprintf((FILE *) fp, "%c", str[i]);
//    fprintf((FILE *) fp, " | %-3d |", c);
//    fprintf((FILE *) fp, "\n");
//}
//
//void save_bitset(void *data, void *fp)
//{
//    auto *p = (Pair *) data;
//    unsigned long key = *(unsigned long *) p->key;
//    unsigned long val = *(unsigned long *) p->val;
//    int c;
//    c = bitcount(val);
//    if (c < 2)
//        return;
//    fprintf((FILE *) fp, "%lu,%lu\n", key, val);
//}

int comp_scores(void *a, void *b)
{
    unsigned long k1 = *(unsigned long *) a;
    unsigned long k2 = *(unsigned long *) b;
    if (k1 > k2)
        return 1;
    else if (k1 < k2)
        return -1;
    else
        return 0;
}

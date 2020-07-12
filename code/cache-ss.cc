#include "cache.h"

long Cache::snapshot_bitmaps(int epoch, int ssr, bool inmem, int experiment)
{
    int i, j;
    long k, l;
    long wbytes = 0;
    FILE *fp;
    char fname[MAX_FILENAME_LEN];
    char tname[MAX_FILENAME_LEN];

    //if (type_bitmap == BITSET)
    //{
    //    for (i = 0; i < num_vdisks; i++)
    //    {
    //        //fprintf(stderr, "Total #objects to persist for vdisk %d: %5d (S:%4lu, M:%4lu)\n", i,
    //        //        bs1[i]->num_nodes,
    //        //        num_obj_hm1[POOL_SINGLE][i], num_obj_hm1[POOL_MULTI][i]);
    //        bs1[i]->save(fp[i]);
    //        //fprintf(stderr, "Bytes written into dump for cached HM1 objects of vdisk %d: %ld\n", i, ftell(dfp[i]));
    //    }
    //    //fprintf(stderr, "Total #objects to persist for HM3: %d\n", bs3->num_nodes);
    //    bs3->save(fp[i]);
    //    //fprintf(stderr, "Bytes written into dump for cached HM3 objects: %ld\n", ftell(dfp[i]));
    //}

    if (type_bitmap == BITARRAY)
    {
        snapshots[epoch] = new Snapshot *[num_vdisks + 1];
        for (i = 0; i < num_vdisks; i++)
        {
            if (!inmem)
            {
                snprintf(fname, MAX_FILENAME_LEN, "%s%s%d/%s/", dump_file_basename, "exp",
                         experiment, wload_name);
                if (!file_exists(fname))
                    mkdir(fname, 0755);
                snprintf(tname, MAX_FILENAME_LEN, "%s%s-%d/", fname, "VDISK", vdisk_ids[i]);
                if (!file_exists(tname))
                    mkdir(tname, 0755);
                snprintf(fname, MAX_FILENAME_LEN, "%s%d/", tname, ssr);
                if (!file_exists(fname))
                    mkdir(fname, 0755);
                snprintf(tname, MAX_FILENAME_LEN, "%s%d.%d.%s", fname, cache_idx, epoch,
                         type_bitmap == BITARRAY ? "bin" : "csv");
                if (!file_exists(tname))
                {
                    fp = fopen(tname, "wb");
                    assert(fp != nullptr);
                    ba1[i]->save_bitmap(fp);
                    fclose(fp);
                }
            }
            else
            {
                snapshots[epoch][i] = new Snapshot(ba1[i]->bits, ba1[i]->bitmap, comp_mem);
            }
            oba1[i]->copy(ba1[i]);
            wbytes += sizeof(ba1[i]->size) + ba1[i]->size * sizeof(unsigned long);
        }
        if (!inmem)
        {
            snprintf(fname, MAX_FILENAME_LEN, "%s%s%d/%s/", dump_file_basename, "exp", experiment, wload_name);
            if (!file_exists(fname))
                mkdir(fname, 0755);
            snprintf(tname, MAX_FILENAME_LEN, "%s%s/", fname, "HM3");
            if (!file_exists(tname))
                mkdir(tname, 0755);
            snprintf(fname, MAX_FILENAME_LEN, "%s%d/", tname, ssr);
            if (!file_exists(fname))
                mkdir(fname, 0755);
            snprintf(tname, MAX_FILENAME_LEN, "%s%d.%d.%s", fname, cache_idx, epoch,
                     type_bitmap == BITARRAY ? "bin" : "csv");
            if (!file_exists(tname))
            {
                fp = fopen(tname, "wb");
                assert(fp != nullptr);
                ba3->save_bitmap(fp);
                fclose(fp);
            }
        }
        else
        {
            snapshots[epoch][i] = new Snapshot(ba3->bits, ba3->bitmap, comp_mem);
            Snapshot::inc_num_snapshots();
        }
        oba3->copy(ba3);
        oba1[i] = oba3;
        wbytes += sizeof(ba3->size) + ba3->size * sizeof(unsigned long);
    }
    return wbytes;
}

void Cache::dump_vdisk(int vdisk, FILE *fp)
{
    int i;
    if (type_bitmap == BITSET)
    {
        bs1[vdisk]->save(fp);
    }
    else if (type_bitmap == BITARRAY)
    {
        ba1[vtl[vdisk]]->save_bitmap(fp);
    }
    fprintf(stderr, "Total #objects to persist for vdisk %d: %5d (S:%4ld, M:%4ld)\n", vdisk,
            bs1[vdisk]->num_nodes,
            cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][vdisk], cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][vdisk]);
    fprintf(stderr, "Bytes written into dump for cached HM1 objects of vdisk %d: %ld\n", vdisk, ftell(fp));
}

long
Cache::load_dump_nopart(struct kv_analyze_all *kvs, long n, long prewarm_set_limit, long *pos,
                        int *prewarm_set_used_size, int *prewarm_set_used_objs)
{
    int j, numex, vdisk;
    long i, k;
    long cursize;
    char *hash;
    struct key_hm1 *khm1;
    struct key_hm3 *khm3;
    Node *node;

    cursize = 0;
    long in = 0;
    // NOT Loading in reverse order
    for (k = *pos; k < n; k++)
    {
        if (kvs[k].vdisk == num_vdisks)
        {
            if (cursize + SIZE_HM3_OBJ <= prewarm_set_limit)
            {
                khm3 = get_hm3_key(kvs[k].key);
                node = hm[HASHMAP3]->search(khm3);
                delete khm3;
                assert(!node->isNULL());
                if (node->data.in_cache(cache_idx))
                {
                    in++;
                    continue;
                }
                if (mem_usage[POOL_MULTI] + SIZE_HM3_OBJ <= mem_limit[POOL_MULTI])
                    fetch_into_multi(&node->data);
                else if (mem_usage[POOL_SINGLE] + SIZE_HM3_OBJ <= mem_limit[POOL_SINGLE])
                    fetch_into_single(&node->data);
                else
                {
                    *pos = n;
                    break;
                }
                numex = to_hm3val((&node->data))->num_extents;
                for (j = 0; j < numex; j++)
                {
                    vdisk = vtl[to_hm3val((&node->data))->extents[j].eid.vdisk];
                    prewarm_set_used_size[vdisk] += SIZE_HM3_OBJ;
                    prewarm_set_used_objs[vdisk]++;
                }
                cursize += SIZE_HM3_OBJ;
            }
            else
                break;
        }
        else
        {
            if (cursize + SIZE_HM1_OBJ <= prewarm_set_limit)
            {
                hash = hashit(vdisk_ids[kvs[k].vdisk], kvs[k].key);
                khm1 = get_hm1_key(vdisk_ids[kvs[k].vdisk], kvs[k].key, hash);
                node = hm[HASHMAP1]->search(khm1);
                delete[] hash;
                delete khm1;
                assert(!node->isNULL());
                if (node->data.in_cache(cache_idx))
                {
                    in++;
                    continue;
                }
                if (mem_usage[POOL_MULTI] + SIZE_HM1_OBJ <= mem_limit[POOL_MULTI])
                    fetch_into_multi(&node->data);
                else if (mem_usage[POOL_SINGLE] + SIZE_HM1_OBJ <= mem_limit[POOL_SINGLE])
                    fetch_into_single(&node->data);
                else
                {
                    *pos = n;
                    break;
                }
                cursize += SIZE_HM1_OBJ;
                prewarm_set_used_size[kvs[k].vdisk] += SIZE_HM1_OBJ;
                prewarm_set_used_objs[kvs[k].vdisk]++;
            }
            else
                break;
        }
    }
    *pos = *pos > k ? *pos : k;
    return prewarm_set_limit - cursize;
}

int Cache::load_dump(BitArray *ba, int vdisk, HMType type, int prewarm_set_share)
{
    long i, j;
    char *hash;
    long vblock;
    long egroup;
    key_hm1 *khm1;
    key_hm3 *khm3;
    Node *node = nullptr;

    // This function saves state by setting bits in 'ba' to 0
    //if (type == HASHMAP1)
    //    ba1[vtl[vdisk]]->clean();
    //else if (type == HASHMAP3)
    //    ba3->clean();

    for (i = 0; i < ba->size; i++)
    {
        if (!ba->bitmap)
            continue;
        for (j = 0; j < 64; j++)
        {
            if (ba->bitmap[i] & (1UL << j))
            {
                if (type == HASHMAP1)
                {
                    vblock = i * 64 + j;
                    hash = hashit(vdisk, vblock);
                    khm1 = get_hm1_key(vdisk, vblock, hash);
                    node = hm[HASHMAP1]->search(khm1);
                    delete[] hash;
                    delete khm1;
                    if (prewarm_set_share - SIZE_HM1_OBJ >= 0)
                        prewarm_set_share -= SIZE_HM1_OBJ;
                    else
                        return prewarm_set_share;
                }
                else if (type == HASHMAP3)
                {
                    egroup = i * 64 + j;
                    khm3 = get_hm3_key(egroup);
                    node = hm[HASHMAP3]->search(khm3);
                    delete khm3;
                    if (prewarm_set_share - SIZE_HM3_OBJ >= 0)
                        prewarm_set_share -= SIZE_HM3_OBJ;
                    else
                        return prewarm_set_share;
                }
                assert(!node->isNULL());

                if (node->data.in_cache(cache_idx))
                {
                    if (type == HASHMAP1)
                    {
                        //ba1[vtl[vdisk]]->set_bit(i * 64 + j);
                        prewarm_set_share += SIZE_HM1_OBJ;
                    }
                    else if (type == HASHMAP3)
                    {
                        //ba3->set_bit(i * 64 + j);
                        prewarm_set_share += SIZE_HM3_OBJ;
                    }
                    ba->unset_bit(i * 64 + j);
                    continue;
                }
                fetch_into_multi(&node->data);
                //if (type == HASHMAP1)
                //    ba1[vtl[vdisk]]->set_bit(i * 64 + j);
                //else if (type == HASHMAP3)
                //    ba3->set_bit(i * 64 + j);
                ba->unset_bit(i * 64 + j);
            }
        }
    }
    return prewarm_set_share;
}

int comp_order(const void *a, const void *b)
{
    long x = ((struct kv_analyze_type *) a)->val;
    long y = ((struct kv_analyze_type *) b)->val;
    if (x < y)
        return 1;
    else if (x > y)
        return -1;
    return 0;
}

long Cache::order_mig_ba(BitArray *ba, void **ret_struct, long *ret_n, int heuristic, int vdisk)
{
    int i, j, k, n, idx;
    char *hash;
    struct key_hm1 *khm1;
    struct val_hm1 *vhm1;
    struct key_hm3 *khm3;
    struct kv_analyze_type *kvs;
    kvs = new struct kv_analyze_type[ba->onbits * 2];
    Node *node;
    n = 0;
    for (i = 0; i < ba->size; i++)
    {
        for (j = 0; j < 64; j++)
        {
            idx = i * 64 + j;
            if (ba->isset(idx))
            {
                kvs[n].key = idx;
                hash = hashit(vdisk, idx);
                khm1 = get_hm1_key(vdisk, idx, hash);
                node = hm[HASHMAP1]->search(khm1);
                delete[] hash;
                delete khm1;
                assert(!node->isNULL());
                kvs[n].val = node->data.meta[cache_idx]->lru.ts;
                kvs[n].type = HASHMAP1;
                n++;

                vhm1 = to_hm1val(&node->data);
                khm3 = get_hm3_key(vhm1->egroup_id);
                node = hm[HASHMAP3]->search(khm3);
                delete khm3;
                assert(!node->isNULL());
                if (node->data.in_cache(cache_idx))
                {
                    kvs[n].key = vhm1->egroup_id;
                    kvs[n].val = node->data.meta[cache_idx]->lru.ts;
                    kvs[n].type = HASHMAP3;
                    kvs[n].pool = node->data.in_multi(cache_idx) ? POOL_MULTI : POOL_SINGLE;
                    n++;
                }
            }
        }
    }
    qsort(kvs, n, sizeof(struct kv_analyze_type), comp_order);

    *ret_struct = (void *) kvs;
    *ret_n = n;
    return 0;
}

long
Cache::load_vdisk_ba(kv_analyze_type *kvs, long n, long prewarm_set_limit, long *pos, int *prewarm_set_used_size,
                     int *prewarm_set_used_objs, int vdisk)
{
    long i, k;
    long cursize;
    char *hash;
    struct key_hm1 *khm1;
    struct key_hm3 *khm3;
    Node *node;

    cursize = 0;
    long in = 0;
    for (k = *pos; k < n; k++)
    {
        if (kvs[k].type == HASHMAP3)
        {
            if (cursize + SIZE_HM3_OBJ <= prewarm_set_limit)
            {
                khm3 = get_hm3_key(kvs[k].key);
                node = hm[HASHMAP3]->search(khm3);
                delete khm3;
                assert(!node->isNULL());
                if (node->data.in_cache(cache_idx))
                {
                    in++;
                    continue;
                }
                cursize += SIZE_HM3_OBJ;
                *prewarm_set_used_size += SIZE_HM3_OBJ;
                (*prewarm_set_used_objs)++;
            }
            else
                continue;
        }
        if (kvs[k].type == HASHMAP1)
        {
            if (cursize + SIZE_HM1_OBJ <= prewarm_set_limit)
            {
                hash = hashit(vdisk, kvs[k].key);
                khm1 = get_hm1_key(vdisk, kvs[k].key, hash);
                node = hm[HASHMAP1]->search(khm1);
                delete[] hash;
                delete khm1;
                assert(!node->isNULL());
                if (node->data.in_cache(cache_idx))
                {
                    in++;
                    continue;
                }
                cursize += SIZE_HM1_OBJ;
                *prewarm_set_used_size += SIZE_HM1_OBJ;
                (*prewarm_set_used_objs)++;
            }
            else
                break;
        }
        if (kvs[k].pool == POOL_MULTI)
            fetch_into_multi(&node->data);
        else
            fetch_into_single(&node->data);
    }
    *pos = k;
    return prewarm_set_limit - cursize;
}
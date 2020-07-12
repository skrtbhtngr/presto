#include "cache.h"

void Cache::fetch_into_single(Pair *val)
{
    int i, c = cache_idx;
    HMType type = val->type;
    Pair *ev = nullptr;
    int vdisk, vblock;
    long egroup;

    if (policy_replace == REPL_LRU)
        insert_lru_list(c, lru_list[POOL_SINGLE], val, counter);
    //else if (policy_replace == REPL_LFU)
    //    insert_lfu_list(c, &lfu_list[POOL_SINGLE], val);

    val->meta[c]->in = IN_SINGLE;
    mem_usage[POOL_SINGLE] += size_obj[type];
    cache_obj_count[POOL_SINGLE][type]++;

    if (type == HASHMAP1)
    {
        vdisk = vtl[to_hm1key(val)->eid.vdisk];
        vblock = to_hm1key(val)->eid.vblock;
        if (type_bitmap == BITARRAY)
            ba1[vdisk]->set_bit(vblock);
        //else if (type_bitmap == BITSET)
        //    bs1[vdisk]->set_bit(vblock);
        cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][vdisk]++;
    }
    else if (type == HASHMAP3)
    {
        egroup = to_hm3key(val)->egroup_id;
        if (type_bitmap == BITARRAY)
            ba3->set_bit(egroup);
        //else if (type_bitmap == BITSET)
        //    bs3->set_bit(egroup);
        for (i = 0; i < to_hm3val(val)->num_extents; i++)
            cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][vtl[to_hm3val(val)->extents[i].eid.vdisk]]++;
    }

    while (mem_usage[POOL_SINGLE] > mem_limit[POOL_SINGLE])
    {
        if (policy_replace == REPL_LRU)
            ev = evict_lru_list(c, lru_list[POOL_SINGLE]);
        //else if (policy_replace == REPL_LFU)
        //    ev = evict_lfu_list(c, &lfu_list[POOL_SINGLE]);
        assert(ev != nullptr);

        ev->meta[c]->in = 0;
        type = ev->type;
        mem_usage[POOL_SINGLE] -= size_obj[type];
        evct_count[POOL_SINGLE][type]++;
        cache_obj_count[POOL_SINGLE][type]--;

        if (type == HASHMAP1)
        {
            vdisk = vtl[to_hm1key(ev)->eid.vdisk];
            vblock = ((key_hm1 *) ev->key)->eid.vblock;
            if (type_bitmap == BITARRAY)
                ba1[vdisk]->unset_bit(vblock);
            //else if (type_bitmap == BITSET)
            //    bs1[vdisk]->unset_bit(vblock);
            cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][vdisk]--;
            evct_count_vdisk_hm1[POOL_SINGLE][vdisk]++;
        }
        else if (type == HASHMAP3)
        {
            egroup = ((key_hm3 *) ev->key)->egroup_id;
            if (type_bitmap == BITARRAY)
                ba3->unset_bit(egroup);
            //else if (type_bitmap == BITSET)
            //    bs3->unset_bit(egroup);
            for (i = 0; i < to_hm3val(ev)->num_extents; i++)
                cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][vtl[to_hm3val(ev)->extents[i].eid.vdisk]]--;
        }
    }
}

void Cache::single_to_multi(Pair *val)
{
    int i, c = cache_idx;
    HMType type = val->type;
    Pair *ev = nullptr;
    int vdisk, vblock;
    long egroup;

    if (policy_replace == REPL_LRU)
    {
        delete_lru_list(c, lru_list[POOL_SINGLE], val);
        insert_lru_list(c, lru_list[POOL_MULTI], val, counter);
    }
    //else if (policy_replace == REPL_LFU)
    //{
    //    delete_lfu_list(c, &lfu_list[POOL_SINGLE], val);
    //    insert_lfu_list(c, &lfu_list[POOL_MULTI], val);
    //}
    cache_obj_count[POOL_SINGLE][type]--;
    mem_usage[POOL_SINGLE] -= size_obj[type];

    val->meta[c]->in = IN_MULTI;
    cache_obj_count[POOL_MULTI][type]++;
    mem_usage[POOL_MULTI] += size_obj[type];

    // No need to set_bit here as the object would have set bit as it was in single pool.
    if (type == HASHMAP1)
    {
        vdisk = vtl[to_hm1key(val)->eid.vdisk];
        vblock = to_hm1key(val)->eid.vblock;
        cache_obj_count_vdisk[POOL_SINGLE][type][vdisk]--;
        cache_obj_count_vdisk[POOL_MULTI][type][vdisk]++;
    }
    else if (type == HASHMAP3)
    {
        egroup = to_hm3key(val)->egroup_id;
        for (i = 0; i < to_hm3val(val)->num_extents; i++)
        {
            cache_obj_count_vdisk[POOL_SINGLE][type][vtl[to_hm3val(val)->extents[i].eid.vdisk]]--;
            cache_obj_count_vdisk[POOL_MULTI][type][vtl[to_hm3val(val)->extents[i].eid.vdisk]]++;
        }
    }

    while (mem_usage[POOL_MULTI] > mem_limit[POOL_MULTI])
    {
        if (policy_replace == REPL_LRU)
            ev = evict_lru_list(c, lru_list[POOL_MULTI]);
        //else if (policy_replace == REPL_LFU)
        //    ev = evict_lfu_list(c, &lfu_list[POOL_MULTI]);
        assert(ev != nullptr);

        ev->meta[c]->in = 0;
        type = ev->type;

        mem_usage[POOL_MULTI] -= size_obj[type];
        evct_count[POOL_MULTI][type]++;
        cache_obj_count[POOL_MULTI][type]--;

        if (type == HASHMAP1)
        {
            vdisk = vtl[to_hm1key(ev)->eid.vdisk];
            vblock = ((key_hm1 *) ev->key)->eid.vblock;
            if (type_bitmap == BITARRAY)
                ba1[vdisk]->unset_bit(vblock);
            //else if (type_bitmap == BITSET)
            //    bs1[vdisk]->unset_bit(vblock);
            cache_obj_count_vdisk[POOL_MULTI][type][vdisk]--;
            evct_count_vdisk_hm1[POOL_MULTI][vdisk]++;
        }
        else if (type == HASHMAP3)
        {
            egroup = to_hm3key(ev)->egroup_id;
            if (type_bitmap == BITARRAY)
                ba3->unset_bit(egroup);
            //else if (type_bitmap == BITSET)
            //    bs3->unset_bit(egroup);
            for (i = 0; i < to_hm3val(ev)->num_extents; i++)
                cache_obj_count_vdisk[POOL_MULTI][type][vtl[to_hm3val(ev)->extents[i].eid.vdisk]]--;
        }
    }
}

void Cache::fetch_into_multi(Pair *val)
{
    int i, c = cache_idx;
    HMType type = val->type;
    Pair *ev = nullptr;
    int vdisk, vblock;
    long egroup;

    if (policy_replace == REPL_LRU)
        insert_lru_list(c, lru_list[POOL_MULTI], val, counter);
    //else if (policy_replace == REPL_LFU)
    //    insert_lfu_list(c, &lfu_list[POOL_MULTI], val);

    val->meta[c]->in = IN_MULTI;
    cache_obj_count[POOL_MULTI][type]++;
    mem_usage[POOL_MULTI] += size_obj[type];

    if (type == HASHMAP1)
    {
        vdisk = vtl[to_hm1key(val)->eid.vdisk];
        vblock = to_hm1key(val)->eid.vblock;
        if (type_bitmap == BITARRAY)
            ba1[vdisk]->set_bit(vblock);
        else if (type_bitmap == BITSET)
            bs1[vdisk]->set_bit(vblock);
        cache_obj_count_vdisk[POOL_MULTI][type][vdisk]++;
    }
    else if (type == HASHMAP3)
    {
        egroup = to_hm3key(val)->egroup_id;
        if (type_bitmap == BITARRAY)
            ba3->set_bit(egroup);
        else if (type_bitmap == BITSET)
            bs3->set_bit(egroup);
        for (i = 0; i < to_hm3val(val)->num_extents; i++)
            cache_obj_count_vdisk[POOL_MULTI][type][vtl[to_hm3val(val)->extents[i].eid.vdisk]]++;
    }

    while (mem_usage[POOL_MULTI] > mem_limit[POOL_MULTI])
    {
        if (policy_replace == REPL_LRU)
            ev = evict_lru_list(c, lru_list[POOL_MULTI]);
        //else if (policy_replace == REPL_LFU)
        //    ev = evict_lfu_list(c, &lfu_list[POOL_MULTI]);
        assert(ev != nullptr);

        ev->meta[c]->in = 0;
        type = ev->type;

        mem_usage[POOL_MULTI] -= size_obj[type];
        evct_count[POOL_MULTI][type]++;
        cache_obj_count[POOL_MULTI][type]--;

        if (type == HASHMAP1)
        {
            vdisk = vtl[to_hm1key(ev)->eid.vdisk];
            vblock = to_hm1key(ev)->eid.vblock;
            if (type_bitmap == BITARRAY)
                ba1[vdisk]->unset_bit(vblock);
            //else if (type_bitmap == BITSET)
            //    bs1[vdisk]->unset_bit(vblock);
            cache_obj_count_vdisk[POOL_MULTI][type][vdisk]--;
            evct_count_vdisk_hm1[POOL_MULTI][vdisk]++;
        }
        else if (type == HASHMAP3)
        {
            egroup = to_hm3key(ev)->egroup_id;
            if (type_bitmap == BITARRAY)
                ba3->unset_bit(egroup);
            //else if (type_bitmap == BITSET)
            //    bs3->unset_bit(egroup);
            for (i = 0; i < to_hm3val(ev)->num_extents; i++)
                cache_obj_count_vdisk[POOL_MULTI][type][vtl[to_hm3val(ev)->extents[i].eid.vdisk]]--;
        }
    }
}

void Cache::touch_pool(Pair *val, int pool)
{
    int c = cache_idx;
    if (policy_replace == REPL_LRU)
    {
        if (lru_list[pool][REAR] != val)
        {
            delete_lru_list(c, lru_list[pool], val);
            insert_lru_list(c, lru_list[pool], val, counter);
        }
    }
    //else if (policy_replace == REPL_LFU)
    //    update_lfu_list(c, &lfu_list[pool], val);
}

void *Cache::lookup(HMType type, void *k, int vdisk)
{
    int c = cache_idx;
    Pair *val = hm[type]->get(k);

    if (val == nullptr)
        return nullptr;
    else if (!val->in_cache(c))
    {
        miss_count_vdisk[type][vtl[vdisk]]++;
        fetch_into_single(val);
    }
    else if (val->in_single(c))
    {
        hit_count_vdisk[POOL_SINGLE][type][vtl[vdisk]]++;
        if (singlepool)
            touch_pool(val, POOL_SINGLE);
        else
            single_to_multi(val);
    }
    else if (val->in_multi(c))
    {
        hit_count_vdisk[POOL_MULTI][type][vtl[vdisk]]++;
        touch_pool(val, POOL_MULTI);
    }
    return val->val;
}

void clean(void *node, void *extra)
{
    Pair *p = (Pair *) node;
    int c = *(int *) extra;
    memset(p->meta[c], 0, sizeof(CacheMeta));
}

void check(void *node, void *extra)
{
    Pair *p = (Pair *) node;
    int c = *(int *) extra;
    assert(p->meta[c]->in == 0);
}

void Cache::reset()
{
    int i, j, k;
    hm[HASHMAP1]->inorder(hm[HASHMAP1]->root, (void *) &cache_idx, &clean);
    hm[HASHMAP3]->inorder(hm[HASHMAP3]->root, (void *) &cache_idx, &clean);
    for (i = 0; i < NUM_POOLS; i++)
    {
        mem_usage[i] = 0;
        for (j = 0; j < num_vdisks; j++)
        {
            evct_count_vdisk_hm1[i][i] = 0;
        }
        for (j = 0; j < NUM_HASHMAPS; j++)
        {
            cache_obj_count[i][j] = 0;
            evct_count[i][j] = 0;
            for (k = 0; k < num_vdisks; k++)
            {
                cache_obj_count_vdisk[i][j][k] = 0;
                hit_count_vdisk[i][j][k] = 0;
            }
        }
        //if (policy_replace == REPL_LFU)
        //{
        //    LFUList *l, *n;
        //    l = lfu_list[i];
        //    while (l != nullptr)
        //    {
        //        n = l;
        //        l = l->next;
        //        delete n;
        //    }
        //    lfu_list[i] = nullptr;
        //}
        if (policy_replace == REPL_LRU)
        {
            for (j = 0; j < Q_PTRS; j++)
                lru_list[i][j] = nullptr;
        }
    }
    for (i = 0; i < NUM_HASHMAPS; i++)
    {
        for (j = 0; j < num_vdisks; j++)
            miss_count_vdisk[i][j] = 0;
    }
    for (i = 0; i < num_vdisks; i++)
    {
        ba1[i]->clean();
        oba1[i]->clean();

        io_count_vdisk[i] = 0;
    }
    ba3->clean();
    oba3->clean();
}

long Cache::delta_bits()
{
    int i, j;
    long ret = 0;
    if (type_bitmap == BITARRAY)
    {
        for (i = 0; i < num_vdisks; i++)
            for (j = 0; j < ba1[i]->size; j++)
                ret += __builtin_popcountll(oba1[i]->bitmap[j] ^ ba1[i]->bitmap[j]);
        for (j = 0; j < ba3->size; j++)
            ret += __builtin_popcountll(oba3->bitmap[j] ^ ba3->bitmap[j]);
    }
    return ret;
}

void Cache::print_cache_stats(FILE *file, bool full)
{
    int i;
    double hit_ratio;
    unsigned long hit, miss;
    unsigned long a, b, c, d, e, f;

    fputc('\n', file);
    fprintf(file, "Cache: %c\n", cache_idx == CACHE_A ? 'A' : 'B');
    fprintf(file, "Replacement Policy: %s\n", policy_replace == REPL_LRU ? "LRU" : "LFU");
    fprintf(file, "Bitmap Type: %s\n", type_bitmap ? "Bit Set" : "Bit Array");

    fputc('\n', file);
    line(41, file);
    fprintf(file, "| %-11s | %10s | %10s |\n", "(objects)", "HASHMAP 1", "HASHMAP 3");
    line(41, file);
    fprintf(file, "| %-11s | %10ld | %10ld |\n", "SINGLE POOL", cache_obj_count[POOL_SINGLE][HASHMAP1],
            cache_obj_count[POOL_SINGLE][HASHMAP3]);
    line(41, file);
    fprintf(file, "| %-11s | %10ld | %10ld |\n", "MULTI POOL", cache_obj_count[POOL_MULTI][HASHMAP1],
            cache_obj_count[POOL_MULTI][HASHMAP3]);
    line(41, file);

    a = b = 0;
    for (i = 0; i < num_vdisks; i++)
    {
        a += miss_count_vdisk[HASHMAP1][i];
        b += miss_count_vdisk[HASHMAP3][i];
    }
    fputc('\n', file);
    line(41, file);
    fprintf(file, "| %-11s | %10s | %10s |\n", "(misses)", "HASHMAP 1", "HASHMAP 3");
    line(41, file);
    fprintf(file, "| %-11s | %10lu | %10lu |\n", "TOTAL", a, b);
    line(41, file);

    a = b = c = d = 0;
    for (i = 0; i < num_vdisks; i++)
    {
        a += hit_count_vdisk[POOL_SINGLE][HASHMAP1][i];
        b += hit_count_vdisk[POOL_SINGLE][HASHMAP3][i];
        c += hit_count_vdisk[POOL_MULTI][HASHMAP1][i];
        d += hit_count_vdisk[POOL_MULTI][HASHMAP3][i];
    }
    fputc('\n', file);
    line(41, file);
    fprintf(file, "| %-11s | %10s | %10s |\n", "(hits)", "HASHMAP 1", "HASHMAP 3");
    line(41, file);
    fprintf(file, "| %-11s | %10lu | %10lu |\n", "SINGLE POOL", a, b);
    line(41, file);
    fprintf(file, "| %-11s | %10lu | %10lu |\n", "MULTI POOL", c, d);
    line(41, file);

    fputc('\n', file);
    line(41, file);
    fprintf(file, "| %-11s | %10s | %10s |\n", "(evictions)", "HASHMAP 1", "HASHMAP 3");
    line(41, file);
    fprintf(file, "| %-11s | %10lu | %10lu |\n", "SINGLE POOL",
            evct_count[POOL_SINGLE][HASHMAP1], evct_count[POOL_SINGLE][HASHMAP3]);
    line(41, file);
    fprintf(file, "| %-11s | %10lu | %10lu |\n", "MULTI POOL",
            evct_count[POOL_MULTI][HASHMAP1], evct_count[POOL_MULTI][HASHMAP3]);
    line(41, file);

    if (!full)
        goto summary;


    fputc('\n', file);
    line(80, file);
    fprintf(file, "| %-11s | %10s | %10s | %10s | %10s | %10s |\n", "(HM1 stats)", "SINGLE", "MULTI",
            "TOTAL HITS",
            "MISSES", "HIT RATIO");
    for (i = 0; i < num_vdisks; i++)
    {
        line(80, file);
        hit_ratio = get_hit_ratio(i);
        fprintf(file,
                "| %-7s %3d | %10lu | %10lu | %10lu | %10lu | %10.3lf |\n",
                "VDISK", vdisk_ids[i], hit_count_vdisk[POOL_SINGLE][HASHMAP1][i],
                hit_count_vdisk[POOL_MULTI][HASHMAP1][i],
                hit_count_vdisk[POOL_SINGLE][HASHMAP1][i] + hit_count_vdisk[POOL_MULTI][HASHMAP1][i],
                miss_count_vdisk[HASHMAP1][i], isnan(hit_ratio) ? 0 : hit_ratio);
    }
    line(80, file);

    fputc('\n', file);
    line(51, file);
    fprintf(file, "| %-11s | %15s | %15s |\n", "(ALL objs)", "HASHMAP 1", "HASHMAP 3");
    for (i = 0; i < num_vdisks; i++)
    {
        line(51, file);
        fprintf(file,
                "| %-7s %3d | %6ld / %-6ld | %6ld / %-6ld |\n",
                "VDISK", vdisk_ids[i],
                cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i] + cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i],
                hashmap_obj_count_vdisk[HASHMAP1][i],
                cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i] + cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i],
                hashmap_obj_count_vdisk[HASHMAP3][i]);
    }
    line(51, file);

    fputc('\n', file);
    line(75, file);
    fprintf(file, "| %-11s | %17s | %19s | %15s |\n", "(ALL objs)", "HASHMAP 1 (MB)", "HASHMAP 3 (MB)",
            "VDISK SIZE (GB)");
    for (i = 0; i < num_vdisks; i++)
    {
        line(75, file);
        fprintf(file,
                "| %-7s %3d | %6.2f / %-8.2f | %7.2f / %-9.2f | %15d |\n",
                "VDISK", vdisk_ids[i],
                ((cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i] + cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i]) *
                 size_obj[HASHMAP1]) / (double) NUM_BYTES_IN_MB,
                ((hashmap_obj_count_vdisk[HASHMAP1][i] * size_obj[HASHMAP1]) / (double) NUM_BYTES_IN_MB),
                (((cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i] + cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i]) *
                  size_obj[HASHMAP3]) / (double) NUM_BYTES_IN_MB),
                ((hashmap_obj_count_vdisk[HASHMAP3][i] * size_obj[HASHMAP3]) / (double) NUM_BYTES_IN_MB),
                vdisk_sizes[i]);
    }
    line(75, file);

    summary:

    fputc('\n', file);
    //mem = (hm[HASHMAP1]->num_nodes * (sizeof(Node) + size_obj[HASHMAP1])) +
    //      (hm[HASHMAP3]->num_nodes) * (sizeof(Node) + size_obj[HASHMAP3]);
    //fprintf(file, "Total memory consumed by the simulation: %.2lf MB\n", mem / (double) NUM_BYTES_IN_MB);
    hit = miss = 0;
    for (i = 0; i < num_vdisks; i++)
    {
        hit += hit_count_vdisk[POOL_SINGLE][HASHMAP1][i] + hit_count_vdisk[POOL_MULTI][HASHMAP1][i]
               + hit_count_vdisk[POOL_SINGLE][HASHMAP3][i] + hit_count_vdisk[POOL_MULTI][HASHMAP3][i];
        miss += miss_count_vdisk[HASHMAP1][i] + miss_count_vdisk[HASHMAP3][i];
    }
    fprintf(file, "Total Hits  : %lu\n", hit);
    fprintf(file, "Total Misses: %lu\n", miss);
    hit_ratio = (double) hit / (hit + miss);
    fprintf(file, "Hit Ratio   : %.3lf\n", hit_ratio);

    fputc('\n', file);
    line(81, file);
}

double Cache::get_hit_ratio()
{
    int i;
    double hit_ratio;
    unsigned long hit, miss;

    hit = miss = 0;
    for (i = 0; i < num_vdisks; i++)
    {
        hit += hit_count_vdisk[POOL_SINGLE][HASHMAP1][i] + hit_count_vdisk[POOL_MULTI][HASHMAP1][i]
               + hit_count_vdisk[POOL_SINGLE][HASHMAP3][i] + hit_count_vdisk[POOL_MULTI][HASHMAP3][i];
        miss += miss_count_vdisk[HASHMAP1][i] + miss_count_vdisk[HASHMAP3][i];
    }
    hit_ratio = (double) hit / (hit + miss);
    return hit_ratio;
}

double Cache::get_hit_ratio(int vdisk)
{
    double hit_ratio;
    unsigned long hit, miss;

    hit = hit_count_vdisk[POOL_SINGLE][HASHMAP1][vdisk] + hit_count_vdisk[POOL_MULTI][HASHMAP1][vdisk] +
          hit_count_vdisk[POOL_SINGLE][HASHMAP3][vdisk] + hit_count_vdisk[POOL_MULTI][HASHMAP3][vdisk];
    miss = miss_count_vdisk[HASHMAP1][vdisk] + miss_count_vdisk[HASHMAP3][vdisk];
    if (hit + miss == 0)
        return 0;
    hit_ratio = (double) hit / (hit + miss);
    return hit_ratio;
}

void Cache::copy(Cache *c)
{
    int i, j;
    Pair *old_it, *new_it, *prev;
    this->cache_idx = c->cache_idx;
    //this->hm[HASHMAP1]->copy(c->hm[HASHMAP1]);
    //this->hm[HASHMAP3]->copy(c->hm[HASHMAP3]);
    //this->policy_replace = REPL_LRU;
    //this->type_bitmap = c->type_bitmap;
    //this->mem_limit[POOL_SINGLE] = c->mem_limit[POOL_SINGLE];
    //this->mem_limit[POOL_MULTI] = c->mem_limit[POOL_MULTI];
    //this->size_obj[HASHMAP1] = c->size_obj[HASHMAP1];
    //this->size_obj[HASHMAP3] = c->size_obj[HASHMAP3];
    //this->num_vdisks = c->num_vdisks;
    //this->wload_name = c->wload_name;
    //this->singlepool = c->singlepool;
    //
    //this->io_count_vdisk = new long[num_vdisks];
    //memcpy(this->io_count_vdisk, c->io_count_vdisk, num_vdisks * sizeof(long));
    //for (i = 0; i < NUM_HASHMAPS; i++)
    //{
    //    this->hashmap_obj_count_vdisk[i] = new long[num_vdisks];
    //    memcpy(this->hashmap_obj_count_vdisk[i], c->hashmap_obj_count_vdisk[i], num_vdisks * sizeof(int));
    //
    //    this->miss_count_vdisk[i] = new long[num_vdisks];
    //    memcpy(this->miss_count_vdisk[i], c->miss_count_vdisk[i], num_vdisks * sizeof(long));
    //}
    //for (i = 0; i < NUM_POOLS; i++)
    //{
    //    this->evct_count_vdisk_hm1[i] = new long[num_vdisks];
    //    memcpy(this->evct_count_vdisk_hm1[i], c->evct_count_vdisk_hm1[i], num_vdisks * sizeof(long));
    //
    //    for (j = 0; j < NUM_HASHMAPS; j++)
    //    {
    //        this->cache_obj_count_vdisk[i][j] = new long[num_vdisks];
    //        memcpy(this->cache_obj_count_vdisk[i][j], c->cache_obj_count_vdisk[i][j], num_vdisks * sizeof(int));
    //
    //        this->hit_count_vdisk[i][j] = new long[num_vdisks];
    //        memcpy(this->hit_count_vdisk[i][j], c->hit_count_vdisk[i][j], num_vdisks * sizeof(long));
    //    }
    //}
    //memcpy(this->evct_count, c->evct_count, sizeof(evct_count));
    //this->vdisk_sizes = new int[c->num_vdisks];
    //memcpy(this->vdisk_sizes, c->vdisk_sizes, c->num_vdisks * sizeof(int));
    //this->vdisk_ids = new int[c->num_vdisks];
    //memcpy(this->vdisk_ids, c->vdisk_ids, c->num_vdisks * sizeof(int));
    //this->vtl = new int[1000];
    //memcpy(this->vtl, c->vtl, 1000 * sizeof(int));
    //
    //if (type_bitmap == BITARRAY)
    //{
    //    this->ba1 = new BitArray *[num_vdisks + 1];
    //    this->oba1 = new BitArray *[num_vdisks + 1];
    //    for (i = 0; i < num_vdisks; i++)
    //    {
    //        this->ba1[i] = new BitArray(c->ba1[i]);
    //        this->oba1[i] = new BitArray(c->oba1[i]);
    //    }
    //    this->ba3 = this->ba1[i] = new BitArray(c->ba1[i]);
    //    this->oba3 = this->oba1[i] = new BitArray(c->oba1[i]);
    //}
    //for (i = 0; i < NUM_POOLS; i++)
    //    for (j = 0; j < Q_PTRS; j++)
    //        this->lru_list[i][j] = c->lru_list[i][j]->meta[cache_idx]->lru.prev;
    //
    //for (i = 0; i < NUM_POOLS; i++)
    //{
    //    old_it = c->lru_list[i][FRONT]->meta[cache_idx]->lru.next;
    //    new_it = this->lru_list[i][FRONT];
    //
    //    this->lru_list[i][FRONT]->meta[cache_idx]->lru.prev = this->lru_list[i][REAR];
    //    this->lru_list[i][REAR]->meta[cache_idx]->lru.next = this->lru_list[i][FRONT];
    //
    //    for (; old_it != c->lru_list[i][FRONT]; old_it = old_it->meta[cache_idx]->lru.next)
    //    {
    //        new_it->meta[cache_idx]->lru.next = old_it->meta[cache_idx]->lru.prev;
    //        new_it->meta[cache_idx]->lru.next->meta[cache_idx]->lru.prev = new_it;
    //    }
    //}
}

void print_facts(FILE *file, int exp, Cache *cache)
{
    fprintf(file, "HASHMAP 1 Object Size: %d B\n", cache->size_obj[HASHMAP1]);
    fprintf(file, "HASHMAP 3 Object Size: %d B\n", cache->size_obj[HASHMAP3]);
    //fprintf(file, "Cache Metadata Size: %lu B\n", sizeof(CacheMeta));
    fputc('\n', file);
    if (exp != 4)
    {
        fprintf(file, "Cache Single Pool Memory Limit (MB): %lu\n", cache->mem_limit[POOL_SINGLE] / NUM_BYTES_IN_MB);
        fprintf(file, "Cache Multi  Pool Memory Limit (MB): %lu\n", cache->mem_limit[POOL_MULTI] / NUM_BYTES_IN_MB);
        fputc('\n', file);
    }
    fprintf(file, "Workload Set: %s\n", cache->wload_name);
    fputc('\n', file);
}

long *Cache::estimate_wss(int window, bool full)
{
    int i, j;
    long *wsses = new long[num_vdisks];
    for (i = 0; i < num_vdisks; i++)
        wsses[i] = 0;
    int vdisk, numex;
    long egroup;
    Pair *it;
    for (i = 0; i < NUM_POOLS; i++)
    {
        it = lru_list[i][REAR];
        while (it != nullptr)
        {
            if (it->meta[cache_idx]->lru.ts < counter - window)
            {
                //fprintf(stderr, "w i: %d, TS: %d\n", i, it->meta[cache_idx]->lru.ts);
                break;
            }
            if (it->type == HASHMAP1)
            {
                vdisk = vtl[to_hm1key(it)->eid.vdisk];
                wsses[vdisk] += SIZE_HM1_OBJ;
            }
            else if (full && it->type == HASHMAP3)
            {
                numex = to_hm3val(it)->num_extents;
                for (j = 0; j < numex; j++)
                {
                    vdisk = vtl[to_hm3val(it)->extents[j].eid.vdisk];
                    wsses[vdisk] += SIZE_HM3_OBJ;
                }
            }
            it = it->meta[cache_idx]->lru.prev;
            if (it == lru_list[i][REAR])
            {
                //fprintf(stderr, "r i: %d, TS: %d\n", i, it->meta[cache_idx]->lru.next->meta[cache_idx]->lru.ts);
                break;
            }
        }
    }
    return wsses;
}
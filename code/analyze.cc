#include "analyze.h"


int comp_analyze(const void *a, const void *b)
{
    long x = ((struct kv_analyze *) a)->val;
    long y = ((struct kv_analyze *) b)->val;
    if (x < y)
        return 1;
    else if (x > y)
        return -1;
    return 0;
}

int comp_analyze_all(const void *a, const void *b)
{
    long x = ((struct kv_analyze_all *) a)->val;
    long y = ((struct kv_analyze_all *) b)->val;
    if (x < y)
        return 1;
    else if (x > y)
        return -1;
    return 0;
}

void analyze_and(BitArray *ba, BitArray *agg)
{
    int i;

    for (i = 0; i < agg->size; i++)
    {
        if (!agg->bitmap[i])
            continue;
        agg->onbits -= __builtin_popcountll(agg->bitmap[i]);
        agg->bitmap[i] &= ba->bitmap[i];
        agg->onbits += __builtin_popcountll(agg->bitmap[i]);
    }
}

void calc_scores_freq(BitArray *ba, BitArray *agg, long *freq, long thr)
{
    int i, j;

    for (i = 0; ba != nullptr && i < ba->size; i++)
    {
        if (!ba->bitmap[i])
            continue;
        for (j = 0; j < 64; j++)
            if (ba->bitmap[i] & (1UL << j))
                freq[i * 64 + j]++;
    }

    if (agg != nullptr)
    {
        for (i = 0; i < agg->size; i++)
        {
            for (j = 0; j < 64; j++)
                if (freq[i * 64 + j] >= thr)
                {
                    agg->bitmap[i] |= (1UL << j);
                    agg->onbits++;
                }
        }
    }
}

void calc_scores_frerecn(BitArray *ba, BitArray *agg, long *scores, long wt, long thr)
{
    int i, j, k, idx, objs;

    for (i = 0; ba != nullptr && i < ba->size; i++)
    {
        if (!ba->bitmap[i])
            continue;
        for (j = 0; j < 64; j++)
            if (ba->bitmap[i] & (1UL << j))
                scores[i * 64 + j] += wt;
    }

    if (agg != nullptr)
    {
        struct kv_analyze *kvs = new struct kv_analyze[agg->size * 64];
        for (i = 0; i < agg->size; i++)
        {
            for (j = 0; j < 64; j++)
            {
                idx = i * 64 + j;
                kvs[idx].key = idx;
                kvs[idx].val = scores[idx];
            }
        }
        qsort(kvs, agg->size * 64, sizeof(struct kv_analyze), comp_analyze);
        for (i = 0; i < agg->size * 64; i++)
            if (kvs[i].val == 0)
                break;
        objs = (i * (thr / 100.0));
        for (k = 0; k < objs && k < (agg->size * 64); k++)
        {
            if (kvs[k].val == 0)
                return;
            i = kvs[k].key / 64;
            j = kvs[k].key % 64;
            agg->bitmap[i] |= (1UL << j);
            agg->onbits++;
        }
        delete[] kvs;
    }
}

long analyze_scores(BitArray *agg, long *scores, int *pos, int obj_size, long thr)
{
    int i, j, k, idx, objs, set;
    struct kv_analyze *kvs = new struct kv_analyze[agg->size * 64];
    for (i = 0; i < agg->size; i++)
    {
        for (j = 0; j < 64; j++)
        {
            idx = i * 64 + j;
            kvs[idx].key = idx;
            kvs[idx].val = scores[idx];
        }
    }
    qsort(kvs, agg->size * 64, sizeof(struct kv_analyze), comp_analyze);
    objs = int(thr / obj_size);
    set = agg->onbits;
    for (k = *pos; k < (*pos + objs) && k < (agg->size * 64); k++)
    {
        if (kvs[k].val == 0)
        {
            *pos = k;
            delete[] kvs;
            return thr - ((long) (agg->onbits - set) * obj_size);
        }
        i = kvs[k].key / 64;
        j = kvs[k].key % 64;
        agg->bitmap[i] |= (1UL << j);
        agg->onbits++;
    }
    *pos = k;
    delete[] kvs;
    return 0;
}

long
analyze_scores_all(BitArray **agg, long **scores, int num_vdisks, long thr, void **ret_struct, long *ret_n,
                   int heuristic)
{
    int i, j, k, n, idx;
    long total_bitmap_size = 0;
    long cursize;
    struct kv_analyze_all *kvs;
    for (i = 0; i <= num_vdisks; i++)
        total_bitmap_size += agg[i]->size;
    total_bitmap_size *= 64;
    kvs = new struct kv_analyze_all[total_bitmap_size];
    n = 0;
    for (k = 0; k <= num_vdisks; k++)
    {
        for (i = 0; i < agg[k]->size; i++)
        {
            for (j = 0; j < 64; j++)
            {
                idx = i * 64 + j;
                if (scores[k][idx] > 0)
                {
                    kvs[n].key = idx;
                    kvs[n].val = scores[k][idx];
                    kvs[n].vdisk = k;
                    n++;
                }
            }
        }
    }
    qsort(kvs, n, sizeof(struct kv_analyze_all), comp_analyze_all);

    cursize = 0;
    for (k = 0; k < n; k++)
    {
        //if (heuristic == K_FREQ_MEM && kvs[k].val < HEURISTIC_FREQ_MIN_VAL)
        //    break;
        if (kvs[k].vdisk == num_vdisks)
        {
            if (cursize + SIZE_HM3_OBJ <= thr)
            {
                i = kvs[k].key / 64;
                j = kvs[k].key % 64;
                agg[num_vdisks]->bitmap[i] |= (1UL << j);
                agg[num_vdisks]->onbits++;
                cursize += SIZE_HM3_OBJ;
            }
            else
                continue;
        }
        else
        {
            if (cursize + SIZE_HM1_OBJ <= thr)
            {
                i = kvs[k].key / 64;
                j = kvs[k].key % 64;
                agg[kvs[k].vdisk]->bitmap[i] |= (1UL << j);
                agg[kvs[k].vdisk]->onbits++;
                cursize += SIZE_HM1_OBJ;
            }
            else
                break;
        }
    }
    *ret_struct = (void *) kvs;
    *ret_n = n;
    return 0;
}

void calc_scores_freq_mem(BitArray *ba, long *scores)
{
    int i, j;

    for (i = 0; ba != nullptr && i < ba->size; i++)
    {
        if (!ba->bitmap[i])
            continue;
        for (j = 0; j < 64; j++)
            if (ba->bitmap[i] & (1UL << j))
                scores[i * 64 + j]++;
    }
}

void calc_scores_rec_mem(BitArray *ba, long *scores, int pos)
{
    int i, j;

    for (i = 0; ba != nullptr && i < ba->size; i++)
    {
        if (!ba->bitmap[i])
            continue;
        for (j = 0; j < 64; j++)
        {
            if (ba->bitmap[i] & (1UL << j) && scores[i * 64 + j] == 0)
                scores[i * 64 + j] = pos;
        }
    }
}

void calc_scores_frerecn_mem(BitArray *ba, long *scores, long wt)
{
    int i, j;

    for (i = 0; ba != nullptr && i < ba->size; i++)
    {
        if (!ba->bitmap[i])
            continue;
        for (j = 0; j < 64; j++)
            if (ba->bitmap[i] & (1UL << j))
                scores[i * 64 + j] += wt;
    }
}

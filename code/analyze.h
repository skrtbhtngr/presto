#pragma once

#include "util.h"
#include "hashmap.h"
#include "cache.h"

struct kv_analyze
{
    int key;
    long val;
};

struct kv_analyze_type
{
    int key;
    long val;
    int type;
    int pool;
};

struct kv_analyze_all
{
    int key;
    long val;
    int vdisk;
};

void analyze_and(BitArray *ba, BitArray *agg);

void calc_scores_freq(BitArray *ba, BitArray *agg, long *freq, long thr);

void calc_scores_frerecn(BitArray *ba, BitArray *agg, long *scores, long wt, long thr);

long analyze_scores(BitArray *agg, long *scores, int *pos, int obj_size, long thr);

long
analyze_scores_all(BitArray **agg, long **scores, int num_vdisks, long thr, void **ret_struct, long *ret_n,
                   int heuristic);

void calc_scores_freq_mem(BitArray *ba, long *scores);

void calc_scores_rec_mem(BitArray *ba, long *scores, int pos);

void calc_scores_frerecn_mem(BitArray *ba, long *scores, long wt);

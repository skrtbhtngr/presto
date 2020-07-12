#pragma once

#include "cache.h"
#include "analyze.h"

struct ns_pair
{
    int val;
    const char *str;
};

struct heuristic_params
{
    int freq;
    int recn;
    int score;
    int frerecn_func;
};

struct part_scheme;

struct run_params
{
    Cache *cache;
    Cache *pcache;
    const char *trace_file;
    const char *base_filename;
    int moment;
    int snapshot_rate;
    int prewarm_rate;
    int heuristic;
    struct heuristic_params hparams;
    long prewarm_set_size_limit;
    int part_scheme_hm1;
    int part_scheme_hm1_hm3;
    double avg_hr;
    int wss_window;
    int wss_stride;
    bool sync_reqs;
    void *rq;

    int vdisk;
    int tpool;
};

struct mig_cache_state
{
    pthread_mutex_t mtx;
    struct kv_analyze_type *kvs;
    long kvs_n;
};

struct async_rq
{
    const char *tfname;
    struct bio_req q[REQ_Q_LEN];
    int front, rear;
    sem_t empty, full;
};

struct sync_rq
{
    const char *tfname;
    struct bio_req *q;
    int idx;
    int used, cursize;
};

void *get_req_from_vdisk_async(void *data);

int get_req_from_q_async(struct bio_req *req, struct async_rq *pc);

void *get_req_from_vdisk_sync(void *data);

int get_req_from_q_sync(struct bio_req *req, struct sync_rq *rq);

int perform_lookup(Cache *cache, struct bio_req *request);

BitArray **analyze_snapshots(Cache *cache, int max_epochs, int ssr, long pre_size_limit, int heuristic,
                             struct heuristic_params hparams, int part_scheme_hm1, int part_scheme_hm1_hm3,
                             int experiment, FILE *debug_file, bool inmem, void **ret_struct, long *ret_n);

void *run_exp2(void *data);

void *run_exp3(void *data);


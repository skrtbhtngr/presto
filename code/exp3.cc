#include "exp3.h"

/*
 * Exp 3:
 * Run the traces on Cache A upto a certain "moment", which means that
 * a particular vdisk migrated after that request.
 * Run the traces on Cache B also up to that "moment".
 *
 * On Cache B, load the cache state for that vdisk.
 * After that, resume the traces on cache B and observe the impact
 * on hit ratio.
 *
 * The importance of a cache object here is determined by
 * its occurrence in the source cache state before migration.
 *
 * The event here is a vm (vdisk) migrating to another node.
 * The cache on the destination node will be cold for this vdisk.
 */
struct mig_cache_state mig;

void exp3()
{
    int pss, pr, hrstc, frfn, pl;
    char res_basefname[MAX_FILENAME_LEN];
    struct sync_rq *rqA, *rqB;

    pthread_t cache_threadA, cache_threadB;
    pthread_t io_threadA, io_threadB;

    Hashmap *hm1 = new Hashmap(&comp_hm1, &print_hm1, nullptr);
    Hashmap *hm3 = new Hashmap(&comp_hm3, &print_hm3, nullptr);
    Workload *wloadA = new Workload("migwload2_6gb"); //<-----------
    Cache *cacheA = new Cache(CACHE_A, hm1, hm3, wloadA, 6144); //<-----------
    Workload *wloadB = new Workload("migwload2_3gb"); //<-----------
    Cache *cacheB = new Cache(CACHE_B, hm1, hm3, wloadB, 3072); //<-----------

    struct ns_pair prewarm_set_size_limit[] = {
            {5,  "5%"},
            {10, "10%"},
            {25, "25%"},
            {50, "50%"},
            {75, "75%"},
    };
    struct ns_pair prewarm_rate_limit[] = {
            {0,   "0MBps"},
            {50,  "50MBps"},
            {100, "100MBps"},
            {500, "500MBps"}
    };
    struct ns_pair heuristic[] = {
            //{K_FREQ_MEM,    "K_FREQ_MEM"},
            {K_RECN_MEM, "K_RECN_MEM"},
            //{K_FRERECN_MEM, "K_FRERECN_MEM"}
    };

    struct ns_pair func_frerecn[] = {
            {LINEAR,    "LNR"},
            {QUADRATIC, "QDR"}
    };

    struct run_params *rpA = new run_params;
    rpA->cache = cacheA;
    rpA->sync_reqs = true;
    rpA->trace_file = wloadA->tracefile;
    rpA->wss_window = 600;
    rpA->wss_stride = 300;
    rpA->moment = 5400;
    rpA->vdisk = 1; // <<----------------- vdisk to migrate

    struct run_params *rpB = new run_params;
    rpB->cache = cacheB;
    rpB->sync_reqs = true;
    rpB->trace_file = wloadB->tracefile;
    rpB->wss_window = 600;
    rpB->wss_stride = 300;
    rpB->moment = 5400;
    rpB->vdisk = 0; // <<----------------- vdisk to migrate

    if (rpA->sync_reqs)
    {
        rqA = new struct sync_rq;
        rqA->tfname = rpA->trace_file;
        rpA->rq = rqA;
        pthread_create(&io_threadA, nullptr, get_req_from_vdisk_sync, (void *) rqA);
        pthread_join(io_threadA, nullptr);
    }
    if (rpB->sync_reqs)
    {
        rqB = new struct sync_rq;
        rqB->tfname = rpB->trace_file;
        rpB->rq = rqB;
        pthread_create(&io_threadB, nullptr, get_req_from_vdisk_sync, (void *) rqB);
        pthread_join(io_threadB, nullptr);
    }
    // Every parameter is for the TARGET cache!!
    for (pss = 0; pss < LEN(prewarm_set_size_limit, ns_pair); pss++)// prewarm set size limit
    {
        rpB->prewarm_set_size_limit = ((prewarm_set_size_limit[pss].val / 100.0) *
                                       (cacheB->total_cache_size / NUM_BYTES_IN_MB));
        fprintf(stderr, "Prewarm set size limit: %ldMB (%s of total cache: %ldMB)\n", rpB->prewarm_set_size_limit,
                prewarm_set_size_limit[pss].str, cacheB->total_cache_size / NUM_BYTES_IN_MB);

        for (pr = 0; pr < LEN(prewarm_rate_limit, ns_pair); pr++)// prewarm rate limit
        {
            rpB->prewarm_rate = prewarm_rate_limit[pr].val;
            fprintf(stderr, "Prewarm rate limit: %s\n", prewarm_rate_limit[pr].str);

            for (hrstc = 0; hrstc < LEN(heuristic, ns_pair); hrstc++)// heuristic
            {
                rpB->heuristic = heuristic[hrstc].val;
                fprintf(stderr, "Heuristic: %s\n", heuristic[hrstc].str);

                if (heuristic[hrstc].val == K_FRERECN_MEM || heuristic[hrstc].val == K_FRERECN)
                {
                    for (frfn = 0; frfn < LEN(func_frerecn, ns_pair); frfn++)// frerecn function
                    {
                        sprintf(res_basefname, PRESTO_BASEPATH "results/%s/%s/%s-%s-%s-%s-%s",
                                "exp3", wloadA->name, prewarm_set_size_limit[pss].str,
                                prewarm_rate_limit[pr].str, heuristic[hrstc].str,
                                func_frerecn[frfn].str, "x");
                        rpA->base_filename = res_basefname;
                        rpA->hparams.frerecn_func = func_frerecn[frfn].val;
                        fprintf(stderr, "File: %s\n", res_basefname);

                        sprintf(res_basefname, PRESTO_BASEPATH "results/%s/%s/%s-%s-%s-%s-%s",
                                "exp3", wloadB->name, prewarm_set_size_limit[pss].str,
                                prewarm_rate_limit[pr].str, heuristic[hrstc].str, func_frerecn[frfn].str, "x");
                        rpB->base_filename = res_basefname;
                        rpB->hparams.frerecn_func = func_frerecn[frfn].val;
                        fprintf(stderr, "File: %s\n", res_basefname);

                        pthread_create(&cache_threadB, nullptr, run_exp3_dest, (void *) rpB);
                        sleep(1);
                        pthread_create(&cache_threadA, nullptr, run_exp3_src, (void *) rpA);
                        pthread_join(cache_threadA, nullptr);
                        pthread_join(cache_threadB, nullptr);
                    }
                }
                else
                {
                    sprintf(res_basefname, PRESTO_BASEPATH "results/%s/%s/%s-%s-%s-%s",
                            "exp3", wloadA->name, prewarm_set_size_limit[pss].str,
                            prewarm_rate_limit[pr].str, heuristic[hrstc].str, "x");
                    rpA->base_filename = res_basefname;
                    fprintf(stderr, "File: %s\n", res_basefname);

                    sprintf(res_basefname, PRESTO_BASEPATH "results/%s/%s/%s-%s-%s-%s",
                            "exp3", wloadB->name, prewarm_set_size_limit[pss].str,
                            prewarm_rate_limit[pr].str, heuristic[hrstc].str, "x");
                    rpB->base_filename = res_basefname;
                    fprintf(stderr, "File: %s\n", res_basefname);

                    pthread_create(&cache_threadB, nullptr, run_exp3_dest, (void *) rpB);
                    sleep(1);
                    pthread_create(&cache_threadA, nullptr, run_exp3_src, (void *) rpA);
                    pthread_join(cache_threadA, nullptr);
                    pthread_join(cache_threadB, nullptr);
                }
            }
        }
    }
}

void *run_exp3_src(void *data)
{
    int i, j;
    int plot_rate = 1;
    int max_plot_epochs, plot_epoch;
    long count, io_data;

    struct run_params *rp = (run_params *) data;
    Cache *cache = rp->cache;
    int num_vdisks = cache->num_vdisks;
    long pre_size;
    struct kv_analyze_type *kvs;
    long kvs_n;

    pthread_t io_thread;

    struct bio_req *request;
    struct async_rq *async_rq;
    struct sync_rq *sync_rq;
    void *rq;
    int (*get_req)(struct bio_req *, void *);

    double **res_hr, res_all_hr[MAX_TIME_SEC + 1];
    long **res_io_count, **res_hm1_miss, **res_hm3_miss;
    int **res_hm1, **res_hm3;

    char hr_fname[MAX_FILENAME_LEN], all_hr_fname[MAX_FILENAME_LEN], all_stats_fname[MAX_FILENAME_LEN],
            obj_fname[MAX_FILENAME_LEN], base_fname[MAX_FILENAME_LEN],
            pre_fname[MAX_FILENAME_LEN];
    FILE *hr_file, *all_hr_file, *all_stats_file, *obj_file, *pre_file;

    if (!file_exists(rp->base_filename))
        mkdir(rp->base_filename, 0755);

    res_hr = new double *[num_vdisks];
    res_io_count = new long *[num_vdisks];
    res_hm1 = new int *[num_vdisks];
    res_hm3 = new int *[num_vdisks];
    res_hm1_miss = new long *[num_vdisks];
    res_hm3_miss = new long *[num_vdisks];;
    for (i = 0; i < num_vdisks; i++)
    {
        res_hr[i] = new double[MAX_TIME_SEC + 1];
        res_io_count[i] = new long[MAX_TIME_SEC + 1];
        res_hm1[i] = new int[MAX_TIME_SEC + 1];
        res_hm3[i] = new int[MAX_TIME_SEC + 1];
        res_hm1_miss[i] = new long[MAX_TIME_SEC + 1];
        res_hm3_miss[i] = new long[MAX_TIME_SEC + 1];
    }

    if (!rp->sync_reqs)
    {
        async_rq = new struct async_rq;
        async_rq->tfname = rp->trace_file;
        sem_init(&async_rq->empty, 0, REQ_Q_LEN);
        sem_init(&async_rq->full, 0, 0);
        async_rq->front = async_rq->rear = 0;
        pthread_create(&io_thread, nullptr, get_req_from_vdisk_async, (void *) async_rq);
        get_req = (int (*)(bio_req *, void *)) (&get_req_from_q_async);
        rq = async_rq;
    }
    else
    {
        get_req = (int (*)(bio_req *, void *)) (&get_req_from_q_sync);
        sync_rq = (struct sync_rq *) rp->rq;
        sync_rq->idx = 0;
        rq = sync_rq;
    }

    request = new struct bio_req;
    sprintf(base_fname, PRESTO_BASEPATH "results/%s/%s/", "exp3", cache->wload_name);

    cache->reset();

    snprintf(all_stats_fname, MAX_FILENAME_LEN, "%s/ALL-STATS.txt", rp->base_filename);
    all_stats_file = fopen(all_stats_fname, "w+");
    assert(all_stats_file != nullptr);

    pthread_mutex_lock(&mig.mtx);

    print_facts(all_stats_file, 1, cache);
    line(81, all_stats_file);
    fprintf(all_stats_file, "Experiment 3\n");

    plot_epoch = 0;

    while (get_req(request, rq))
    {
        count += perform_lookup(cache, request);

        if (request->ts >= (plot_epoch + 1) * plot_rate)
        {
            for (i = 0; i < num_vdisks; i++)
            {
                res_hr[i][plot_epoch] = cache->get_hit_ratio(i);
                res_hm1[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i];
                res_hm3[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i];
                res_hm1_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP1][i];
                res_hm3_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP3][i];
                res_io_count[i][plot_epoch] = cache->io_count_vdisk[i];
            }
            res_all_hr[plot_epoch] = cache->get_hit_ratio();
            plot_epoch++;
        }

        if (request->ts >= rp->moment)
            break;
    }
    if (!rp->sync_reqs)
    {
        pthread_join(io_thread, nullptr);
        delete async_rq;
    }

    for (i = 0; i < num_vdisks; i++)
    {
        res_hr[i][plot_epoch] = cache->get_hit_ratio(i);
        res_hm1[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i]
                                 + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i];
        res_hm3[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i]
                                 + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i];
        res_hm1_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP1][i];
        res_hm3_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP3][i];
        res_io_count[i][plot_epoch] = cache->io_count_vdisk[i];
    }
    res_all_hr[plot_epoch] = cache->get_hit_ratio();

    max_plot_epochs = plot_epoch + 1;
    for (i = 0; i < num_vdisks; i++)
    {
        snprintf(hr_fname, MAX_FILENAME_LEN, "%sCOLD-VDISK-%d-HR.csv", base_fname, cache->vdisk_ids[i]);
        snprintf(obj_fname, MAX_FILENAME_LEN, "%sCOLD-VDISK-%d-OBJ.csv", base_fname, cache->vdisk_ids[i]);
        hr_file = fopen(hr_fname, "w+");
        assert(hr_file != nullptr);
        obj_file = fopen(obj_fname, "w+");
        assert(obj_file != nullptr);
        for (j = 0; j < max_plot_epochs; j++)
        {
            fprintf(hr_file, "%d,%lf\n", j, res_hr[i][j]);
            fprintf(obj_file, "%d,%ld,%d,%d,%ld,%ld\n", j, res_io_count[i][j], res_hm1[i][j], res_hm3[i][j],
                    res_hm1_miss[i][j], res_hm3_miss[i][j]);
        }
        fflush(hr_file);
        fclose(hr_file);
        fflush(obj_file);
        fclose(obj_file);
    }
    snprintf(all_hr_fname, MAX_FILENAME_LEN, "%s/COLD-ALL-HR.csv", base_fname);
    all_hr_file = fopen(all_hr_fname, "w+");
    assert(all_hr_file != nullptr);
    for (j = 0; j < max_plot_epochs; j++)
        fprintf(all_hr_file, "%d,%lf\n", j, res_all_hr[j]);
    fflush(all_hr_file);
    fclose(all_hr_file);

    cache->order_mig_ba(cache->ba1[rp->vdisk], (void **) &kvs, &kvs_n, K_RECN_MEM, cache->vdisk_ids[rp->vdisk]);
    mig.kvs = kvs;
    mig.kvs_n = kvs_n;
    pre_size = kvs_n * SIZE_HM1_OBJ;

    fprintf(all_stats_file, "Hit Ratio of the VM just before migration: %lf\n", cache->get_hit_ratio(rp->vdisk));
    fprintf(all_stats_file, "Migrated prewarm set total size %.2lf MB\n", pre_size / (double) NUM_BYTES_IN_MB);
    fprintf(all_stats_file, "Total requests recevied at source: %lu\n", count);

    cache->print_cache_stats(all_stats_file, true);
    line(81, all_stats_file);

    pthread_mutex_unlock(&mig.mtx);

    return nullptr;
}

void *run_exp3_dest(void *data)
{
    int i, j, k;
    int plot_rate = 1;
    int max_plot_epochs, plot_epoch;
    long count, io_data;
    int counter;

    struct run_params *rp = (run_params *) data;
    Cache *cache = rp->cache;
    int num_vdisks = cache->num_vdisks;
    int heuristic = rp->heuristic;
    long pre_size_limit = rp->prewarm_set_size_limit;
    long prewarm_rate = (rp->prewarm_rate == 0 ? pre_size_limit : rp->prewarm_rate) * NUM_BYTES_IN_MB;
    pre_size_limit *= NUM_BYTES_IN_MB;
    long kvs_n, pos_pwm;

    int wss_window = rp->wss_window;
    int wss_stride = rp->wss_stride;
    int wss_stride_epoch, max_wss_stride_epochs;
    long *wsses;

    pthread_t io_thread;

    struct bio_req *request;
    int prewarm_set_used_size;
    int prewarm_set_used_objs;
    long prewarm_set_used_total;
    struct async_rq *async_rq;
    struct sync_rq *sync_rq;
    void *rq;
    int (*get_req)(struct bio_req *, void *);

    double **res_hr, res_all_hr[MAX_TIME_SEC + 1];
    long **res_io_count, **res_hm1_miss, **res_hm3_miss, **res_wss, *res_usage;
    int **res_hm1, **res_hm3;

    char hr_fname[MAX_FILENAME_LEN], all_hr_fname[MAX_FILENAME_LEN], all_stats_fname[MAX_FILENAME_LEN],
            obj_fname[MAX_FILENAME_LEN], base_fname[MAX_FILENAME_LEN], pre_fname[MAX_FILENAME_LEN],
            wss_fname[MAX_FILENAME_LEN], usage_fname[MAX_FILENAME_LEN];;
    FILE *hr_file, *all_hr_file, *all_stats_file, *obj_file, *pre_file, *wss_file, *usage_file;

    if (!file_exists(rp->base_filename))
        mkdir(rp->base_filename, 0755);

    res_hr = new double *[num_vdisks];
    res_io_count = new long *[num_vdisks];
    res_hm1 = new int *[num_vdisks];
    res_hm3 = new int *[num_vdisks];
    res_hm1_miss = new long *[num_vdisks];
    res_hm3_miss = new long *[num_vdisks];;
    res_wss = new long *[num_vdisks];
    for (i = 0; i < num_vdisks; i++)
    {
        res_hr[i] = new double[MAX_TIME_SEC + 1];
        res_io_count[i] = new long[MAX_TIME_SEC + 1];
        res_hm1[i] = new int[MAX_TIME_SEC + 1];
        res_hm3[i] = new int[MAX_TIME_SEC + 1];
        res_hm1_miss[i] = new long[MAX_TIME_SEC + 1];
        res_hm3_miss[i] = new long[MAX_TIME_SEC + 1];
        res_wss[i] = new long[MAX_TIME_SEC + 2 - wss_stride];
    }
    res_usage = new long[MAX_TIME_SEC + 1];

    if (!rp->sync_reqs)
    {
        async_rq = new struct async_rq;
        async_rq->tfname = rp->trace_file;
        sem_init(&async_rq->empty, 0, REQ_Q_LEN);
        sem_init(&async_rq->full, 0, 0);
        async_rq->front = async_rq->rear = 0;
        pthread_create(&io_thread, nullptr, get_req_from_vdisk_async, (void *) async_rq);
        get_req = (int (*)(bio_req *, void *)) (&get_req_from_q_async);
        rq = async_rq;
    }
    else
    {
        get_req = (int (*)(bio_req *, void *)) (&get_req_from_q_sync);
        sync_rq = (struct sync_rq *) rp->rq;
        sync_rq->idx = 0;
        rq = sync_rq;
    }

    request = new struct bio_req;
    sprintf(base_fname, PRESTO_BASEPATH "results/%s/%s/", "exp3", cache->wload_name);

    cache->reset();

    snprintf(all_stats_fname, MAX_FILENAME_LEN, "%s/ALL-STATS.txt", rp->base_filename);
    all_stats_file = fopen(all_stats_fname, "w+");
    assert(all_stats_file != nullptr);

    print_facts(all_stats_file, 1, cache);
    line(81, all_stats_file);
    fprintf(all_stats_file, "Experiment 3\n");

    ////////////////////////////////////////////////////////////////////////////////////////////
    //// WITHOUT PREWARMING
    ////////////////////////////////////////////////////////////////////////////////////////////

    plot_epoch = wss_stride_epoch = 0;

    while (get_req(request, rq))
    {
        if (request->vdisk == cache->vdisk_ids[rp->vdisk])
            continue;

        count += perform_lookup(cache, request);

        if (request->ts >= (plot_epoch + 1) * plot_rate)
        {
            for (i = 0; i < num_vdisks; i++)
            {
                res_hr[i][plot_epoch] = cache->get_hit_ratio(i);
                res_hm1[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i];
                res_hm3[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i];
                res_hm1_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP1][i];
                res_hm3_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP3][i];
                res_io_count[i][plot_epoch] = cache->io_count_vdisk[i];
            }
            res_all_hr[plot_epoch] = cache->get_hit_ratio();
            res_usage[plot_epoch] = cache->mem_usage[POOL_SINGLE] + cache->mem_usage[POOL_MULTI];
            plot_epoch++;
        }
        if (request->ts >= (wss_stride_epoch + 1) * wss_stride)
        {
            if (request->ts >= wss_window)
            {
                wsses = cache->estimate_wss(wss_window, true);
                for (i = 0; i < num_vdisks; i++)
                    res_wss[i][wss_stride_epoch] = wsses[i];
                delete wsses;
            }
            wss_stride_epoch++;
        }

        if (request->ts >= rp->moment)
        {
            fprintf(all_stats_file, "VM migrated after request: %lu\n", count);
            break;
        }
    }
    while (get_req(request, rq))
    {
        count += perform_lookup(cache, request);

        if (request->ts >= (plot_epoch + 1) * plot_rate)
        {
            for (i = 0; i < num_vdisks; i++)
            {
                res_hr[i][plot_epoch] = cache->get_hit_ratio(i);
                res_hm1[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i];
                res_hm3[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i];
                res_hm1_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP1][i];
                res_hm3_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP3][i];
                res_io_count[i][plot_epoch] = cache->io_count_vdisk[i];
            }
            res_all_hr[plot_epoch] = cache->get_hit_ratio();
            res_usage[plot_epoch] = cache->mem_usage[POOL_SINGLE] + cache->mem_usage[POOL_MULTI];
            plot_epoch++;
        }
    }
    if (!rp->sync_reqs)
    {
        pthread_join(io_thread, nullptr);
        delete async_rq;
    }

    for (i = 0; i < num_vdisks; i++)
    {
        res_hr[i][plot_epoch] = cache->get_hit_ratio(i);
        res_hm1[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i]
                                 + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i];
        res_hm3[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i]
                                 + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i];
        res_hm1_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP1][i];
        res_hm3_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP3][i];
        res_io_count[i][plot_epoch] = cache->io_count_vdisk[i];
    }
    res_all_hr[plot_epoch] = cache->get_hit_ratio();
    res_usage[plot_epoch] = cache->mem_usage[POOL_SINGLE] + cache->mem_usage[POOL_MULTI];

    max_wss_stride_epochs = wss_stride_epoch + 1;
    max_plot_epochs = plot_epoch + 1;

    for (i = 0; i < num_vdisks; i++)
    {
        snprintf(hr_fname, MAX_FILENAME_LEN, "%sCOLD-VDISK-%d-HR.csv", base_fname, cache->vdisk_ids[i]);
        snprintf(obj_fname, MAX_FILENAME_LEN, "%sCOLD-VDISK-%d-OBJ.csv", base_fname, cache->vdisk_ids[i]);
        snprintf(wss_fname, MAX_FILENAME_LEN, "%sCOLD-VDISK-%d-WSS-%d-%d.csv", base_fname, cache->vdisk_ids[i],
                 rp->wss_window, rp->wss_stride);
        hr_file = fopen(hr_fname, "w+");
        assert(hr_file != nullptr);
        obj_file = fopen(obj_fname, "w+");
        assert(obj_file != nullptr);
        wss_file = fopen(wss_fname, "w+");
        assert(wss_file != nullptr);
        for (j = 0; j < max_plot_epochs; j++)
        {
            fprintf(hr_file, "%d,%lf\n", j, res_hr[i][j]);
            fprintf(obj_file, "%d,%ld,%d,%d,%ld,%ld\n", j, res_io_count[i][j], res_hm1[i][j], res_hm3[i][j],
                    res_hm1_miss[i][j], res_hm3_miss[i][j]);
        }
        for (j = 0; j < max_wss_stride_epochs; j++)
            fprintf(wss_file, "%d,%ld\n", j, res_wss[i][j]);

        fflush(hr_file);
        fclose(hr_file);
        fflush(obj_file);
        fclose(obj_file);
        fflush(wss_file);
        fclose(wss_file);
    }
    snprintf(usage_fname, MAX_FILENAME_LEN, "%s/COLD-ALL-USAGE.csv", base_fname);
    usage_file = fopen(usage_fname, "w+");
    assert(usage_file != nullptr);
    for (j = 0; j < max_plot_epochs; j++)
        fprintf(usage_file, "%d,%ld\n", j, res_usage[j]);
    fflush(usage_file);
    fclose(usage_file);

    snprintf(all_hr_fname, MAX_FILENAME_LEN, "%s/COLD-ALL-HR.csv", base_fname);
    all_hr_file = fopen(all_hr_fname, "w+");
    assert(all_hr_file != nullptr);
    for (j = 0; j < max_plot_epochs; j++)
        fprintf(all_hr_file, "%d,%lf\n", j, res_all_hr[j]);
    fflush(all_hr_file);
    fclose(all_hr_file);

    cache->print_cache_stats(all_stats_file, true);

    ////////////////////////////////////////////////////////////////////////////////////////////
    //// WITH PREWARMING
    ////////////////////////////////////////////////////////////////////////////////////////////

    cache->reset();

    if (!rp->sync_reqs)
    {
        sem_init(&async_rq->empty, 0, REQ_Q_LEN);
        sem_init(&async_rq->full, 0, 0);
        async_rq->front = async_rq->rear = 0;
        pthread_create(&io_thread, nullptr, get_req_from_vdisk_async, (void *) async_rq);
        get_req = (int (*)(bio_req *, void *)) (&get_req_from_q_async);
        rq = async_rq;
    }
    else
    {
        get_req = (int (*)(bio_req *, void *)) (&get_req_from_q_sync);
        sync_rq = (struct sync_rq *) rp->rq;
        sync_rq->idx = 0;
        rq = sync_rq;
    }

    count = plot_epoch = 0;

    while (get_req(request, rq))
    {
        if (request->vdisk == cache->vdisk_ids[rp->vdisk])
            continue;

        count += perform_lookup(cache, request);

        if (request->ts >= (plot_epoch + 1) * plot_rate)
        {
            for (i = 0; i < num_vdisks; i++)
            {
                res_hr[i][plot_epoch] = cache->get_hit_ratio(i);
                res_hm1[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i];
                res_hm3[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i];
                res_hm1_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP1][i];
                res_hm3_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP3][i];
                res_io_count[i][plot_epoch] = cache->io_count_vdisk[i];
            }
            res_all_hr[plot_epoch] = cache->get_hit_ratio();
            plot_epoch++;
        }

        if (request->ts >= rp->moment)
        {
            fprintf(all_stats_file, "VM migrated after request: %lu\n", count);
            break;
        }
    }
    pthread_mutex_lock(&mig.mtx);
    assert (mig.kvs != nullptr);
    prewarm_set_used_size = prewarm_set_used_objs = pos_pwm = 0;
    counter = rp->moment;

    while (get_req(request, rq))
    {
        while (request->ts >= counter && pre_size_limit >= SIZE_HM1_OBJ)
        {
            if (prewarm_rate > pre_size_limit)
                prewarm_rate = pre_size_limit;

            k = cache->load_vdisk_ba(mig.kvs, mig.kvs_n, prewarm_rate, &pos_pwm, &prewarm_set_used_size,
                                     &prewarm_set_used_objs, cache->vdisk_ids[rp->vdisk]);
            prewarm_set_used_total = prewarm_rate - k;
            pre_size_limit -= prewarm_set_used_total;

            if (pos_pwm == mig.kvs_n)
                pre_size_limit = 0;

            if (prewarm_set_used_total > 0)
            {
                fprintf(stderr, "Total size of objects loaded at %d second: %.2lf MB\n", counter,
                        prewarm_set_used_total / (double) NUM_BYTES_IN_MB);

                snprintf(pre_fname, MAX_FILENAME_LEN, "%s/%ds-SHARE.csv", rp->base_filename, counter);
                pre_file = fopen(pre_fname, "w+");
                assert(pre_file != nullptr);
                fprintf(pre_file, "%d,%u,%u\n", cache->vdisk_ids[rp->vdisk], prewarm_set_used_size,
                        prewarm_set_used_objs);
                fflush(pre_file);
                fclose(pre_file);
            }
            counter++;
        }

        count += perform_lookup(cache, request);

        if (request->ts >= (plot_epoch + 1) * plot_rate)
        {
            for (i = 0; i < num_vdisks; i++)
            {
                res_hr[i][plot_epoch] = cache->get_hit_ratio(i);
                res_hm1[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i];
                res_hm3[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i]
                                         + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i];
                res_hm1_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP1][i];
                res_hm3_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP3][i];
                res_io_count[i][plot_epoch] = cache->io_count_vdisk[i];
            }
            res_all_hr[plot_epoch] = cache->get_hit_ratio();
            plot_epoch++;
        }
    }
    if (!rp->sync_reqs)
    {
        pthread_join(io_thread, nullptr);
        delete async_rq;
    }
    for (i = 0; i < num_vdisks; i++)
    {
        res_hr[i][plot_epoch] = cache->get_hit_ratio(i);
        res_hm1[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP1][i]
                                 + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP1][i];
        res_hm3[i][plot_epoch] = cache->cache_obj_count_vdisk[POOL_SINGLE][HASHMAP3][i]
                                 + cache->cache_obj_count_vdisk[POOL_MULTI][HASHMAP3][i];
        res_hm1_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP1][i];
        res_hm3_miss[i][plot_epoch] = cache->miss_count_vdisk[HASHMAP3][i];
        res_io_count[i][plot_epoch] = cache->io_count_vdisk[i];
    }
    res_all_hr[plot_epoch] = cache->get_hit_ratio();

    max_plot_epochs = plot_epoch + 1;
    cache->print_cache_stats(all_stats_file, true);

    for (i = 0; i < num_vdisks; i++)
    {
        snprintf(hr_fname, MAX_FILENAME_LEN, "%s/VDISK-%d-HR.csv", rp->base_filename, cache->vdisk_ids[i]);
        snprintf(obj_fname, MAX_FILENAME_LEN, "%s/VDISK-%d-OBJ.csv", rp->base_filename, cache->vdisk_ids[i]);
        hr_file = fopen(hr_fname, "w+");
        assert(hr_file != nullptr);
        obj_file = fopen(obj_fname, "w+");
        assert(obj_file != nullptr);
        for (j = 0; j < max_plot_epochs; j++)
        {
            fprintf(hr_file, "%d,%lf\n", j, res_hr[i][j]);
            fprintf(obj_file, "%d,%ld,%d,%d,%ld,%ld\n", j, res_io_count[i][j], res_hm1[i][j], res_hm3[i][j],
                    res_hm1_miss[i][j], res_hm3_miss[i][j]);
        }
        fflush(hr_file);
        fclose(hr_file);
        fflush(obj_file);
        fclose(obj_file);
    }
    snprintf(all_hr_fname, MAX_FILENAME_LEN, "%s/ALL-HR.csv", rp->base_filename);
    all_hr_file = fopen(all_hr_fname, "w+");
    assert(all_hr_file != nullptr);
    for (j = 0; j < max_plot_epochs; j++)
        fprintf(all_hr_file, "%d,%lf\n", j, res_all_hr[j]);
    fflush(all_hr_file);
    fclose(all_hr_file);

    line(81, all_stats_file);
    fflush(all_stats_file);
    fclose(all_stats_file);

    fprintf(stderr, "\n\n");

    pthread_mutex_unlock(&mig.mtx);

    return nullptr;
}
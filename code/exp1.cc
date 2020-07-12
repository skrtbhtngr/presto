#include "exp1.h"

/*
 * Exp 1:
 * Run the traces and dump the cache state periodically at
 * the snapshot_rate of n requests.
 * Then analyze_and those dumps to determine most important
 * objects and prewarm the cache with them.
 *
 * After that, run the traces again and observe the impact
 * on hit ratio.
 *
 * The importance of a cache object here is measured in terms
 * of number of times it appears in all dumps.
 *
 * The event here is a powered-off node (and all its vms) booting up
 * (after we had analyzed the cache snapshots for an earlier run).
 */

void exp1()
{
    int ssr, pss, pr, hrstc, scr, frfn;
    char res_basefname[MAX_FILENAME_LEN];
    char *basedirname;

    pthread_t cache_thread;

    Workload *wload = new Workload("test_iops100"); //<-----------
    Hashmap *hm1 = new Hashmap(&comp_hm1, &print_hm1, nullptr);
    Hashmap *hm3 = new Hashmap(&comp_hm3, &print_hm3, nullptr);
    Cache *cache = new Cache(CACHE_A, hm1, hm3, wload, 3000); //<-----------
    Hashmap *phm1 = new Hashmap(&comp_hm1, &print_hm1, nullptr);
    Hashmap *phm3 = new Hashmap(&comp_hm3, &print_hm3, nullptr);
    Cache *pcache = new Cache();
    pcache->hm[HASHMAP1] = phm1;
    pcache->hm[HASHMAP3] = phm3;

    struct ns_pair snapshot_rate[] = {
            //{5000,   "5000"},
            {10000,  "10000"},
            {50000,  "50000"},
            {100000, "100000"},
            //{500000, "500K"},
            //{5,      "5s"},
            //{10,     "10s"},
            //{15,     "15s"},
            //{30,     "30s"},
            //{60,     "1m"},
            //{300,    "5m"}
    };
    struct ns_pair prewarm_set_size_limit[] = {
            {5,   "5%"},
            {10,  "10%"},
            {25,  "25%"},
            {50,  "50%"},
            {75,  "75%"},
            {100, "100%"}
    };
    struct ns_pair prewarm_rate_limit[] = {
            {0, "0MBps"},
            //{1000, "100MBps"},
            //{2000, "200MBps"}
    };
    struct ns_pair heuristic[] = {
            //{K_FREQ,        "K_FREQ"},
            {K_FREQ_MEM,    "K_FREQ_MEM"},
            //{K_RECN,        "K_RECN"},
            {K_RECN_MEM,    "K_RECN_MEM"},
            //{K_FRERECN,     "K_FRERECN"},
            {K_FRERECN_MEM, "K_FRERECN_MEM"}
    };
    struct ns_pair score_recn[] = {
            {1, "1"},
            {2, "2"},
            {3, "3"},
            {4, "4"},
            {5, "5"},
    };
    struct ns_pair score_freq[] = {
            {100, "100%"},
            {50,  "50%"},
            {33,  "33%"},
            {25,  "25%"},
            {20,  "20%"},
    };
    struct ns_pair score_frerecn[] = {
            {1,  "1%"},
            {5,  "5%"},
            {10, "10%"},
            {15, "15%"},
            {20, "20%"}
    };
    struct ns_pair *score;

    struct ns_pair func_frerecn[] = {
            {LINEAR,    "LNR"},
            {QUADRATIC, "QDR"}
    };

    struct run_params *rp = new run_params;
    rp->cache = cache;
    rp->pcache = pcache;
    rp->part_scheme_hm1 = HM1_NO_PART;
    rp->part_scheme_hm1_hm3 = HM1_HM3_NO_PART;
    rp->trace_file = wload->tracefile;
    rp->wss_window = 600;
    rp->wss_stride = 300;

    asprintf(&basedirname, PRESTO_BASEPATH "results/%s/%s/", "exp1", wload->name);
    if (!file_exists(basedirname))
        mkdir(basedirname, 0755);

    for (ssr = 0; ssr < LEN(snapshot_rate, ns_pair); ssr++)// snapshot rate
    {
        rp->snapshot_rate = snapshot_rate[ssr].val;
        fprintf(stderr, "Snapshot rate: %s\n", snapshot_rate[ssr].str);
        asprintf(&basedirname, PRESTO_BASEPATH "results/%s/%s/%s/", "exp1", wload->name, snapshot_rate[ssr].str);
        if (!file_exists(basedirname))
            mkdir(basedirname, 0755);

        for (pss = 0; pss < LEN(prewarm_set_size_limit, ns_pair); pss++)// prewarm set size limit
        {
            rp->prewarm_set_size_limit = ((prewarm_set_size_limit[pss].val / 100.0) *
                                          (cache->total_cache_size / NUM_BYTES_IN_MB));
            if (rp->prewarm_set_size_limit == cache->total_cache_size / NUM_BYTES_IN_MB)
                rp->prewarm_set_size_limit--;
            fprintf(stderr, "Prewarm set size limit: %luMB (%s of total cache: %luMB)\n", rp->prewarm_set_size_limit,
                    prewarm_set_size_limit[pss].str, cache->total_cache_size / NUM_BYTES_IN_MB);

            for (pr = 0; pr < LEN(prewarm_rate_limit, ns_pair); pr++)// prewarm rate limit
            {
                rp->prewarm_rate = prewarm_rate_limit[pr].val;
                fprintf(stderr, "Prewarm rate limit: %s\n", prewarm_rate_limit[pr].str);

                for (hrstc = 0; hrstc < LEN(heuristic, ns_pair); hrstc++)// heuristic
                {
                    rp->heuristic = heuristic[hrstc].val;
                    fprintf(stderr, "Heuristic: %s\n", heuristic[hrstc].str);

                    for (scr = 0; scr < LEN(score_frerecn, ns_pair); scr++)// heuristic score
                    {
                        switch (heuristic[hrstc].val)
                        {
                            case K_RECN:
                                score = score_recn;
                                rp->hparams.recn = score[scr].val;
                                break;
                            case K_FRERECN:
                                score = score_frerecn;
                                rp->hparams.score = score[scr].val;
                                break;
                            case K_FREQ:
                                score = score_freq;
                                rp->hparams.freq = score[scr].val;
                                break;
                            default:
                                score = nullptr;
                                break;
                        }

                        if (heuristic[hrstc].val == K_FRERECN_MEM || heuristic[hrstc].val == K_FRERECN)
                        {
                            for (frfn = 0; frfn < LEN(func_frerecn, ns_pair); frfn++)// frerecn function
                            {
                                sprintf(res_basefname, PRESTO_BASEPATH "results/%s/%s/%s/%s-%s-%s-%s-%s",
                                        "exp1", wload->name, snapshot_rate[ssr].str,
                                        prewarm_set_size_limit[pss].str,
                                        prewarm_rate_limit[pr].str, heuristic[hrstc].str,
                                        func_frerecn[scr].str, score ? score[scr].str : "x");
                                rp->base_filename = res_basefname;
                                rp->hparams.frerecn_func = func_frerecn[scr].val;
                                fprintf(stderr, "File: %s\n", res_basefname);
                                pthread_create(&cache_thread, nullptr, run_exp1, (void *) rp);
                                pthread_join(cache_thread, nullptr);
                            }
                        }
                        else
                        {
                            sprintf(res_basefname, PRESTO_BASEPATH "results/%s/%s/%s/%s-%s-%s-%s",
                                    "exp1", wload->name, snapshot_rate[ssr].str, prewarm_set_size_limit[pss].str,
                                    prewarm_rate_limit[pr].str, heuristic[hrstc].str, score ? score[scr].str : "x");
                            rp->base_filename = res_basefname;
                            fprintf(stderr, "File: %s\n", res_basefname);
                            pthread_create(&cache_thread, nullptr, run_exp1, (void *) rp);
                            pthread_join(cache_thread, nullptr);
                        }

                        if (heuristic[hrstc].val == K_FREQ_MEM ||
                            heuristic[hrstc].val == K_RECN_MEM ||
                            heuristic[hrstc].val == K_FRERECN_MEM)
                            break;
                    }
                }
            }
        }
    }
}

void *run_exp1(void *data)
{
    int i, j, k;
    int epoch, max_epochs;
    int plot_rate = 1;
    int max_plot_epochs, plot_epoch;
    long count, io_data;
    int counter;
    bool inmem;

    struct run_params *rp = (run_params *) data;
    Cache *cache = rp->cache;
    Cache *pcache = rp->pcache;
    int num_vdisks = cache->num_vdisks;
    int snapshot_rate = rp->snapshot_rate;
    int heuristic = rp->heuristic;
    struct heuristic_params hparams = rp->hparams;
    long pre_size_limit = rp->prewarm_set_size_limit;
    int part_scheme_hm1 = rp->part_scheme_hm1;
    int part_scheme_hm1_hm3 = rp->part_scheme_hm1_hm3;
    long prewarm_rate = (rp->prewarm_rate == 0 ? pre_size_limit : rp->prewarm_rate) * NUM_BYTES_IN_MB;
    struct kv_analyze_all *kvs;
    long kvs_n, *pos;

    int wss_window = rp->wss_window;
    int wss_stride = rp->wss_stride;
    int wss_stride_epoch, max_wss_stride_epochs;
    long *wsses;

    pthread_t io_thread;

    struct bio_req *request;
    int *prewarm_set_share;
    int *prewarm_set_share_rem;
    int rem_vdisks_prewarm;
    int *prewarm_set_used;
    long prewarm_set_used_total;
    struct bio_req *req;
    struct async_rq *rq;

    double **res_hr, res_all_hr[MAX_TIME_SEC + 1];
    long **res_io_count, **res_hm1_miss, **res_hm3_miss, **res_wss;
    int **res_hm1, **res_hm3;

    BitArray **agg;
    long wbytes = 0;
    char hr_fname[MAX_FILENAME_LEN], all_hr_fname[MAX_FILENAME_LEN], all_stats_fname[MAX_FILENAME_LEN],
            diff_fname[MAX_FILENAME_LEN], obj_fname[MAX_FILENAME_LEN], base_fname[MAX_FILENAME_LEN],
            pre_fname[MAX_FILENAME_LEN], wss_fname[MAX_FILENAME_LEN];
    FILE *hr_file, *all_hr_file, *all_stats_file, *diff_file, *obj_file, *pre_file, *wss_file;

    if (!file_exists(rp->base_filename))
        mkdir(rp->base_filename, 0755);

    snprintf(all_stats_fname, MAX_FILENAME_LEN, "%s/ALL-STATS.txt", rp->base_filename);
    snprintf(diff_fname, MAX_FILENAME_LEN, "/home/skrtbhtngr/CLionProjects/presto/results/exp1/%s/%d.diff",
             cache->wload_name, snapshot_rate);

    res_hr = new double *[num_vdisks];
    res_io_count = new long *[num_vdisks];
    res_hm1 = new int *[num_vdisks];
    res_hm3 = new int *[num_vdisks];
    res_hm1_miss = new long *[num_vdisks];
    res_hm3_miss = new long *[num_vdisks];
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

    if (!file_exists(diff_fname))
    {
        diff_file = fopen(diff_fname, "w+");
        assert(diff_file != nullptr);
    }
    else
        diff_file = nullptr;

    all_stats_file = fopen(all_stats_fname, "w+");
    assert(all_stats_file != nullptr);

    rq = new struct async_rq;
    rq->tfname = rp->trace_file;

    inmem = false;

    print_facts(all_stats_file, 1, cache);
    line(81, all_stats_file);
    fprintf(all_stats_file, "Experiment 1\n");
    if (snapshot_rate >= 1000)
        fprintf(all_stats_file, "Snapshot rate: %dK requests\n", snapshot_rate / 1000);
    else
        fprintf(all_stats_file, "Snapshot rate: %d seconds\n", snapshot_rate);

    request = new struct bio_req;

    sprintf(base_fname, PRESTO_BASEPATH "results/%s/%s/%d/", "exp1", cache->wload_name, snapshot_rate);


    // If pcache is present, skip the first half!
    if (pcache->cache_idx != -1 && pcache->run_ssr == snapshot_rate)
    {
        //cache->copy(pcache);
        max_epochs = pcache->run_max_epochs;
        goto prewarm_step;
    }
    cache->reset();

    fprintf(stderr, "Starting run with cold cache...\n");

    sem_init(&rq->empty, 0, REQ_Q_LEN);
    sem_init(&rq->full, 0, 0);
    rq->front = rq->rear = 0;
    pthread_create(&io_thread, nullptr, get_req_from_vdisk_async, (void *) rq);

    count = counter = epoch = plot_epoch = 0;
    wss_stride_epoch = 0;

    while (get_req_from_q_async(request, rq))
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

        if (request->ts >= (wss_stride_epoch + 1) * wss_stride)
        {
            if (request->ts >= wss_window)
            {
                //long sum = 0;
                fprintf(stderr, "Started WSS Estimation at time %d sec...\n", cache->counter);
                wsses = cache->estimate_wss(wss_window, true);
                for (i = 0; i < num_vdisks; i++)
                {
                    res_wss[i][wss_stride_epoch] = wsses[i];
                    //sum += (long) wsses[i];
                    fprintf(stderr, "WSS of VDISK %d(%d): %.2lf MB\n", i, cache->vdisk_ids[i], wsses[i] / 1048576.0);
                }
                //fprintf(stderr, "Total: %.2lf MB\n", sum / 1048576.0);
                delete wsses;
            }
            wss_stride_epoch++;
        }

        //if (snapshot_rate >= 1000)
        //{
        //    if (count > (epoch + 1) * snapshot_rate)
        //    {
        //        if (diff_file)
        //            fprintf(diff_file, "%d,%lu\n", epoch, cache->delta_bits());
        //        wbytes += cache->snapshot_bitmaps(epoch, snapshot_rate, inmem, EXP1);
        //        epoch++;
        //    }
        //}
        //else
        //{
        //    if (request->ts >= (counter + 1) * snapshot_rate)
        //    {
        //        if (diff_file)
        //            fprintf(diff_file, "%d,%lu\n", epoch, cache->delta_bits());
        //        wbytes += cache->snapshot_bitmaps(epoch, snapshot_rate, inmem, EXP1);
        //        counter++;
        //    }
        //}
    }
    pthread_join(io_thread, nullptr);

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

    wbytes += cache->snapshot_bitmaps(epoch, snapshot_rate, inmem, EXP1);
    max_epochs = epoch + 1;
    max_plot_epochs = plot_epoch + 1;
    max_wss_stride_epochs = wss_stride_epoch + 1;

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
    snprintf(all_hr_fname, MAX_FILENAME_LEN, "%s/COLD-ALL-HR.csv", base_fname);
    all_hr_file = fopen(all_hr_fname, "w+");
    assert(all_hr_file != nullptr);
    for (j = 0; j < max_plot_epochs; j++)
        fprintf(all_hr_file, "%d,%lf\n", j, res_all_hr[j]);
    fflush(all_hr_file);
    fclose(all_hr_file);

    if (diff_file)
        fclose(diff_file);

    pcache->copy(cache);
    pcache->run_ssr = snapshot_rate;
    pcache->run_max_epochs = max_epochs;

    prewarm_step:

    fprintf(all_stats_file, "Total number of vdisks served: %d \n", num_vdisks);
    fprintf(all_stats_file, "Total requests received: %lu\n", count);
    fprintf(all_stats_file, "Total number of snapshots taken (per vdisk/HM3): %d (%ld total)\n", max_epochs,
            (num_vdisks + 1L) * max_epochs);
    fprintf(all_stats_file, "Total HM1 objects in the metadata store: %d\n", cache->hm[HASHMAP1]->num_nodes);
    fprintf(all_stats_file, "Total HM3 objects in the metadata store: %d\n", cache->hm[HASHMAP3]->num_nodes);
    if (!inmem)
        fprintf(all_stats_file, "Total size of cache state written to the disk: %.2lf MB\n", round(wbytes / 1048576.0));

    cache->print_cache_stats(all_stats_file, true);
    line(81, all_stats_file);

    cache->reset();

    fprintf(stderr, "Starting heuristic-based snapshot analysis...\n");
    ////////////////
    agg = analyze_snapshots(cache, max_epochs, snapshot_rate, pre_size_limit, heuristic, hparams, part_scheme_hm1,
                            part_scheme_hm1_hm3, EXP1, all_stats_file, inmem, (void **) &kvs, &kvs_n);
    if (agg == nullptr)
    {
        fclose(hr_file);
        fclose(all_stats_file);
        fprintf(stderr, "agg in NULL! Exiting...\n");
        return nullptr;
    }
    ////////////////

    rem_vdisks_prewarm = num_vdisks + 1;
    prewarm_set_share = new int[num_vdisks + 1];
    prewarm_set_share_rem = new int[num_vdisks + 1];
    prewarm_set_used = new int[num_vdisks + 1];
    for (i = 0; i <= num_vdisks; i++)
        prewarm_set_share_rem[i] = agg[i]->onbits * (i == num_vdisks ? SIZE_HM3_OBJ : SIZE_HM1_OBJ);
    pos = new long;
    *pos = 0;

    sem_init(&rq->empty, 0, REQ_Q_LEN);
    sem_init(&rq->full, 0, 0);
    rq->front = rq->rear = 0;
    pthread_create(&io_thread, nullptr, get_req_from_vdisk_async, (void *) rq);

    fprintf(stderr, "Starting run with prewarmed cache...\n");
    count = counter = plot_epoch = 0;

    while (get_req_from_q_async(request, rq))
    {
        while (request->ts >= (counter + 1) && rem_vdisks_prewarm > 0)
        {
            prewarm_set_used_total = 0;
            for (i = 0; i <= num_vdisks; i++)
                prewarm_set_used[i] = 0;
            if (part_scheme_hm1 == HM1_NO_PART && part_scheme_hm1_hm3 == HM1_HM3_NO_PART)
            {
                k = cache->load_dump_nopart(kvs, kvs_n, prewarm_rate, pos, prewarm_set_used, nullptr);
                prewarm_set_used_total = prewarm_rate - k;

                // need this to change
                rem_vdisks_prewarm = 0;
            }
            else
            {
                for (i = 0; i <= num_vdisks; i++)
                {
                    if (prewarm_set_share_rem[i] == 0)
                        continue;
                    prewarm_set_share[i] = prewarm_rate / rem_vdisks_prewarm; // <---- how to distribute the pss??
                }
                for (i = 0; i <= num_vdisks; i++)
                {
                    if (prewarm_set_share_rem[i] == 0)
                        continue;
                    k = cache->load_dump(agg[i], cache->vdisk_ids[i], i == num_vdisks ? HASHMAP3 : HASHMAP1,
                                         prewarm_set_share[i]);
                    prewarm_set_used_total += prewarm_set_share[i] - k;
                    prewarm_set_used[i] += prewarm_set_share[i] - k;
                    if (k != 0)
                    {
                        rem_vdisks_prewarm--;
                        prewarm_set_share_rem[i] = prewarm_set_share[i] = 0;
                    }
                    else
                        prewarm_set_share_rem[i] -= prewarm_set_share[i];
                }
            }

            if (prewarm_set_used_total > 0)
            {
                fprintf(stderr, "Total size of objects loaded at %d second: %.2lf MB\n", counter,
                        prewarm_set_used_total / (double) NUM_BYTES_IN_MB);

                snprintf(pre_fname, MAX_FILENAME_LEN, "%s/%ds-SHARE.csv", rp->base_filename, counter);
                pre_file = fopen(pre_fname, "w+");
                assert(pre_file != nullptr);
                for (i = 0; i <= num_vdisks; i++)
                {
                    fprintf(pre_file, "%d,%u,%u\n", i == num_vdisks ? 0 : cache->vdisk_ids[i], prewarm_set_used[i],
                            prewarm_set_used[i] / (i == num_vdisks ? SIZE_HM3_OBJ : SIZE_HM1_OBJ));
                }
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
    pthread_join(io_thread, nullptr);

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

    for (i = 0; i <= num_vdisks; i++)
        delete agg[i];

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
    return nullptr;
}
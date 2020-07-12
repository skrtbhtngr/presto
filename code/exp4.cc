#include "exp4.h"

/*
 * Exp 4:
 * Determine the minimal cache size needed
 * per-vDisk to get an average 0.8 or 0.9
 * hit ratio in the whole duration.
 */
void exp4()
{
    int vdisk, csize;
    char res_basefname[MAX_FILENAME_LEN];
    char res_basedirname[MAX_FILENAME_LEN];
    pthread_t cache_thread, io_thread;
    struct sync_rq *rq;

    Workload *wload = new Workload("umass.4hr");
    Hashmap *hm1 = new Hashmap(&comp_hm1, &print_hm1, nullptr);
    Hashmap *hm3 = new Hashmap(&comp_hm3, &print_hm3, nullptr);
    Cache *cache = new Cache(CACHE_A, hm1, hm3, wload, -1);

    int cache_sizes[] = {
            1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
            125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450, 475, 500,
            550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1100, 1200, 1300, 1400, 1500,
            1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900, 3000
    };
    double hr_x = 0, hr_y = 0;

    cache->singlepool = true;
    cache->mem_limit[POOL_MULTI] = 0;

    struct run_params *rp = new run_params;
    rp->cache = cache;
    rp->sync_reqs = true;
    sprintf(res_basedirname, PRESTO_BASEPATH "results/%s/%s", "exp4", wload->name);

    // Create the directory for this workloads set if it does not exist.
    if (!file_exists(res_basedirname))
        mkdir(res_basedirname, 0755);

    for (vdisk = 0; vdisk < wload->num_vdisks; vdisk++)
    {
        fprintf(stderr, "WLOAD: %s\n", wload->name);
        fprintf(stderr, "VDISK: %d\n", wload->vdisks[vdisk].vdisk_id);
        rp->vdisk = wload->vdisks[vdisk].vdisk_id;
        rp->trace_file = wload->vdisks[vdisk].trace_file;
        if (rp->sync_reqs)
        {
            rq = new struct sync_rq;
            rq->tfname = rp->trace_file;
            rp->rq = rq;
            pthread_create(&io_thread, nullptr, get_req_from_vdisk_sync, (void *) rq);
            pthread_join(io_thread, nullptr);
        }

        for (csize = 0; csize < LEN(cache_sizes, int); csize++)
        {
            fprintf(stderr, "CACHE MEMORY LIMIT: %d MB\n", cache_sizes[csize]);
            sprintf(res_basedirname, PRESTO_BASEPATH "results/%s/%s/VDISK-%d",
                    "exp4", wload->name, wload->vdisks[vdisk].vdisk_id);

            // Create the directory for this VDISK if it does not exist.
            if (!file_exists(res_basedirname))
                mkdir(res_basedirname, 0755);

            sprintf(res_basefname, PRESTO_BASEPATH "results/%s/%s/VDISK-%d/%dM",
                    "exp4", wload->name, wload->vdisks[vdisk].vdisk_id, cache_sizes[csize]);
            rp->base_filename = res_basefname;
            cache->mem_limit[POOL_SINGLE] = cache_sizes[csize] * NUM_BYTES_IN_MB;

            pthread_create(&cache_thread, nullptr, run_exp4, (void *) rp);
            pthread_join(cache_thread, nullptr);

            if (fabs(hr_x - 0) < 1E-6)
            {
                hr_x = rp->avg_hr;
                continue;
            }
            hr_y = hr_x;
            hr_x = rp->avg_hr;
            if (fabs(hr_x - hr_y) < 1E-6)
            {
                fprintf(stderr, "No improvement in Hit Ratio. Breaking early at %d MB cache size!\n",
                        cache_sizes[csize]);
                hr_x = 0;
                break;
            }
        }
        if (rp->sync_reqs)
        {
            free(rq->q);
            delete rq;
        }
    }
}

void *run_exp4(void *data)
{
    int i;
    unsigned long count, io_data;
    pthread_t io_thread;

    struct run_params *rp = (run_params *) data;
    Cache *cache = rp->cache;
    int vdisk = rp->vdisk;

    int plot_rate = 1;
    int max_plot_epochs, plot_epoch;
    struct bio_req *request;
    struct async_rq *async_rq;
    struct sync_rq *sync_rq;
    void *rq;
    int (*get_req)(struct bio_req *, void *);

    double res_hr[MAX_TIME_SEC + 1];
    double avg_hr;
    int res_hm1[MAX_TIME_SEC + 1], res_hm3[MAX_TIME_SEC + 1];
    unsigned long res_io_count[MAX_TIME_SEC + 1], res_io_data[MAX_TIME_SEC + 1];
    char hrfile[MAX_FILENAME_LEN], allfile[MAX_FILENAME_LEN], objfile[MAX_FILENAME_LEN];
    FILE *fp_hr, *fp_all, *fp_obj;

    snprintf(hrfile, MAX_FILENAME_LEN, "%s-HR.csv", rp->base_filename);
    snprintf(objfile, MAX_FILENAME_LEN, "%s-OBJ.csv", rp->base_filename);
    snprintf(allfile, MAX_FILENAME_LEN, "%s-STATS.txt", rp->base_filename);

    // Don't run the experiment for this VDISK and parameter combination if the result files exist
    if (file_exists(hrfile) || file_exists(objfile) || file_exists(allfile))
    {
        fprintf(stderr, "Result file(s) exist for VDISK %d!\n", vdisk);
        return nullptr;
    }

    fp_hr = fopen(hrfile, "w+");
    assert(fp_hr != nullptr);
    fp_obj = fopen(objfile, "w+");
    assert(fp_obj != nullptr);
    fp_all = fopen(allfile, "w+");
    assert(fp_all != nullptr);

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

    count = plot_epoch = io_data = 0;

    cache->reset();

    print_facts(fp_all, 4, cache);
    line(81, fp_all);
    fprintf(fp_all, "Experiment 4\n");

    request = new struct bio_req;

    while (get_req(request, rq))
    {
        count += perform_lookup(cache, request);
        io_data += request->size;

        while (request->ts >= ((unsigned) plot_epoch + 1) * plot_rate)
        {
            res_hr[plot_epoch] = cache->get_hit_ratio(cache->vtl[request->vdisk]);
            res_hm1[plot_epoch] = cache->cache_obj_count[POOL_SINGLE][HASHMAP1];
            res_hm3[plot_epoch] = cache->cache_obj_count[POOL_SINGLE][HASHMAP3];
            res_io_count[plot_epoch] = count;
            res_io_data[plot_epoch] = io_data;
            plot_epoch++;
        }
    }
    if (!rp->sync_reqs)
    {
        pthread_join(io_thread, nullptr);
        delete async_rq;
    }

    res_hr[plot_epoch] = cache->get_hit_ratio(cache->vtl[request->vdisk]);
    res_hm1[plot_epoch] = cache->cache_obj_count[POOL_SINGLE][HASHMAP1];
    res_hm3[plot_epoch] = cache->cache_obj_count[POOL_SINGLE][HASHMAP3];
    res_io_count[plot_epoch] = count;
    res_io_data[plot_epoch] = io_data;
    max_plot_epochs = plot_epoch + 1;
    delete request;

    fprintf(fp_all, "Total requests received: %lu in %.2f hrs\n", count, max_plot_epochs / 3600.0);
    fprintf(fp_all, "Total HM1 objects in the metadata store: %d\n", cache->hm[HASHMAP1]->num_nodes);
    fprintf(fp_all, "Total HM3 objects in the metadata store: %d\n", cache->hm[HASHMAP3]->num_nodes);

    cache->print_cache_stats(fp_all, false);
    line(81, fp_all);
    fflush(fp_all);
    fclose(fp_all);

    avg_hr = 0;
    for (i = 0; i < max_plot_epochs; i++)
    {
        avg_hr += res_hr[i];
        fprintf(fp_hr, "%d,%lf\n", i, res_hr[i]);
        fprintf(fp_obj, "%d,%d,%d,%lu,%lu\n", i, res_hm1[i], res_hm3[i],
                res_io_count[i], res_io_data[i]);
    }
    avg_hr /= max_plot_epochs;
    rp->avg_hr = avg_hr;

    fflush(fp_obj);
    fclose(fp_obj);
    fflush(fp_hr);
    fclose(fp_hr);

    return nullptr;
}
#include "experiments.h"
#include "util.h"
#include "cache.h"

pthread_mutex_t mig_mtx = PTHREAD_MUTEX_INITIALIZER;


void *get_req_from_vdisk_async(void *data)
{
    int sval1, sval2;
    struct async_rq *pc = (struct async_rq *) data;
    const char *tf = pc->tfname;
    char buf[MAX_REQ_LENGTH], *word, *saveptr;
    struct bio_req *req;
    long offset;

    FILE *fp = fopen(tf, "r");
    assert(fp != nullptr);
    while (fgets(buf, MAX_REQ_LENGTH, fp))
    {
        sem_wait(&pc->empty);
        req = &pc->q[pc->rear];
        pc->rear = (pc->rear + 1) % REQ_Q_LEN;

        word = strtok_r(buf, ",", &saveptr);
        //req->type = word[0];
        word = strtok_r(nullptr, ",", &saveptr);
        req->vdisk = strtol(word, nullptr, 0);
        word = strtok_r(nullptr, ",", &saveptr);;
        offset = strtol(word, nullptr, 0);
        offset *= 512UL;
        word = strtok_r(nullptr, ",", &saveptr);
        req->size = strtol(word, nullptr, 0);
        req->vblock = (int) (offset / NUM_BYTES_IN_EXTENT);
        req->vblock_offset = (int) (offset % NUM_BYTES_IN_EXTENT);
        word = strtok_r(nullptr, ",", &saveptr);
        req->ts = strtod(word, nullptr);

        sem_post(&pc->full);
        if (req->ts >= MAX_TIME_SEC)
        {
            sem_wait(&pc->full);
            sem_post(&pc->empty);
            break;
        }
    }
    pc->rear = -1;
    fclose(fp);
    return nullptr;
}

int get_req_from_q_async(struct bio_req *req, struct async_rq *pc)
{
    int sval;
    if (pc->rear == -1)
    {
        sem_getvalue(&pc->full, &sval);
        if (sval == 0)
            return 0;
    }
    sem_wait(&pc->full);
    //req->type = (&pc->q[pc->front])->type;
    req->vdisk = (&pc->q[pc->front])->vdisk;
    //req->offset = (&pc->q[pc->front])->offset;
    req->size = (&pc->q[pc->front])->size;
    req->vblock = (&pc->q[pc->front])->vblock;
    req->vblock_offset = (&pc->q[pc->front])->vblock_offset;
    req->ts = (&pc->q[pc->front])->ts;
    pc->front = (pc->front + 1) % REQ_Q_LEN;
    sem_post(&pc->empty);
    return 1;
}

void *get_req_from_vdisk_sync(void *data)
{
    int i;
    struct sync_rq *rq = (struct sync_rq *) data;
    const char *tf = rq->tfname;
    char buf[MAX_REQ_LENGTH], *word, *saveptr;
    long offset;

    FILE *fp = fopen(tf, "r");
    assert(fp != nullptr);
    i = 0;
    rq->used = 0;
    rq->cursize = 65536;
    rq->q = (struct bio_req *) malloc(rq->cursize * sizeof(struct bio_req));
    while (fgets(buf, MAX_REQ_LENGTH, fp))
    {
        word = strtok_r(buf, ",", &saveptr);
        //req->type = word[0];
        word = strtok_r(nullptr, ",", &saveptr);
        rq->q[i].vdisk = strtol(word, nullptr, 0);
        word = strtok_r(nullptr, ",", &saveptr);;
        offset = strtol(word, nullptr, 0);
        offset *= 512UL;
        word = strtok_r(nullptr, ",", &saveptr);
        rq->q[i].size = strtol(word, nullptr, 0);
        rq->q[i].vblock = (int) (offset / NUM_BYTES_IN_EXTENT);
        rq->q[i].vblock_offset = (int) (offset % NUM_BYTES_IN_EXTENT);
        word = strtok_r(nullptr, ",", &saveptr);
        rq->q[i].ts = strtod(word, nullptr);

        if (rq->q[i].ts >= MAX_TIME_SEC)
            break;

        i++;
        if (i >= rq->cursize)
        {
            rq->cursize *= 2;
            rq->q = (struct bio_req *) realloc(rq->q, rq->cursize * sizeof(struct bio_req));
            assert(rq->q != nullptr);
        }
    }
    rq->used = i;
    fclose(fp);
    return nullptr;
}

int get_req_from_q_sync(struct bio_req *req, struct sync_rq *rq)
{
    if (rq->idx == rq->used)
        return 0;
    req->vdisk = (&rq->q[rq->idx])->vdisk;
    req->size = (&rq->q[rq->idx])->size;
    req->vblock = (&rq->q[rq->idx])->vblock;
    req->vblock_offset = (&rq->q[rq->idx])->vblock_offset;
    req->ts = (&rq->q[rq->idx])->ts;
    rq->idx++;
    return 1;
}


/*int get_next_request_real(FILE **fp, const char *basename)
{
    //static int i;
    char fname[MAX_FILENAME_LEN];
    char buf[MAX_REQ_LENGTH], *word, *saveptr;

    if (*fp == nullptr)
    {
        //sprintf(fname, "%s%d.csv", basename);
        *fp = fopen(basename, "r");
        if (!*fp)
        {
            fprintf(stderr, "Cannot open file: %s\n", strerror(errno));
            return -1;
        }
    }

    if (!fgets(buf, MAX_REQ_LENGTH, *fp))
    {
        //if (i == 3)
        //{
        //    fclose(*fp);
        //    *fp = nullptr;
        //    return -1;
        //}
        //else
        //{
        //    fclose(*fp);
        //    sprintf(fname, "%s%d.csv", basename, ++i);
        //    *fp = fopen(fname, "r");
        //    fgets(buf, MAX_REQ_LENGTH, *fp);
        //}
        return -1;
    }

    word = strtok_r(buf, ",", &saveptr);
    req.type = word[0];
    word = strtok_r(nullptr, ",", &saveptr);
    req.vdisk = strtol(word, nullptr, 0);
    word = strtok_r(nullptr, ",", &saveptr);;
    req.offset = strtol(word, nullptr, 0);
    req.offset *= 512ULL;
    word = strtok_r(nullptr, ",", &saveptr);
    req.mem_usage = strtol(word, nullptr, 0);
    req.vblock = req.offset / NUM_BYTES_IN_EXTENT;
    req.vblock_offset = req.offset % NUM_BYTES_IN_EXTENT;
    word = strtok_r(nullptr, ",", &saveptr);
    req.ts = strtod(word, nullptr);
    return 0;
}*/

int perform_lookup(Cache *cache, struct bio_req *request)
{
    int i, count = 0;
    long size;
    char *hash;

    key_hm1 *khm1;
    val_hm1 *vhm1;

    key_hm3 *khm3;
    val_hm3 *vhm3;

    while (request->ts >= cache->counter + 1)
        cache->counter++;

    //fprintf(stderr, "--------------------------------------------------------------------------\n");
    size = request->size;
    request->size = min(request->size, (signed) NUM_BYTES_IN_MB - request->vblock_offset);
    hash = hashit(request->vdisk, request->vblock);

    again:
    //fprintf(stderr, "%c %u %9lu %9lu %9lu %s\n",
    //        request->type, request->vdisk, request->vblock, request->vblock_offset, request->mem_usage, hash);

    count++;
    khm1 = get_hm1_key(request->vdisk, request->vblock, hash);
    vhm1 = (val_hm1 *) cache->lookup(HASHMAP1, khm1, request->vdisk);
    if (vhm1 == nullptr)
    {
        vhm1 = get_hm1_val(request->vdisk, request->vblock, hash);
        cache->hm[HASHMAP1]->put(khm1, vhm1, HASHMAP1);
        cache->hashmap_obj_count_vdisk[HASHMAP1][cache->vtl[request->vdisk]]++;
        vhm1 = (val_hm1 *) cache->lookup(HASHMAP1, khm1, request->vdisk);
        assert(vhm1 != nullptr);
    }
    else
        delete khm1;

    delete[] hash;

    khm3 = get_hm3_key(vhm1->egroup_id);
    vhm3 = (val_hm3 *) cache->lookup(HASHMAP3, khm3, request->vdisk);
    if (vhm3 == nullptr)
    {
        vhm3 = get_hm3_val(vhm1->egroup_id, request->vdisk, request->vblock, request->vblock_offset, request->size);
        cache->hm[HASHMAP3]->put(khm3, vhm3, HASHMAP3);
        cache->hashmap_obj_count_vdisk[HASHMAP3][cache->vtl[request->vdisk]]++;
        vhm3 = (val_hm3 *) cache->lookup(HASHMAP3, khm3, request->vdisk);
        assert(vhm3 != nullptr);
    }
    else
    {
        delete khm3;
        for (i = 0; i < NUM_EXTENTS_IN_EGROUP; i++)
            if (vhm3->extents[i].eid.vdisk == request->vdisk &&
                vhm3->extents[i].eid.vblock == request->vblock)
                break;
        if (i == NUM_EXTENTS_IN_EGROUP)
        {
            update_hm3_val(vhm3, vhm1->egroup_id, request->vdisk, request->vblock, request->vblock_offset,
                           request->size);
            cache->hashmap_obj_count_vdisk[HASHMAP3][cache->vtl[request->vdisk]]++;
        }
    }

    /* We have a request which spans over two vblocks (maybe greater than 1 MB too) */
    size -= request->size;
    if (size > 0)
    {
        request->vblock_offset = 0;
        request->size = min(size, NUM_BYTES_IN_MB);
        request->vblock++;
        hash = hashit(request->vdisk, request->vblock);
        goto again;
    }
    cache->io_count_vdisk[cache->vtl[request->vdisk]] += count;

    return count;
}

BitArray **analyze_snapshots(Cache *cache, int max_epochs, int ssr, long pre_size_limit, int heuristic,
                             struct heuristic_params hparams, int part_scheme_hm1, int part_scheme_hm1_hm3,
                             int experiment, FILE *debug_file, bool inmem, void **ret_struct, long *ret_n)
{
    int i, j, epoch, num_vdisks, last_ss;
    long k, l, onbits1, onbits3, total_objs;
    char fname[MAX_FILENAME_LEN];
    int freq = hparams.freq;
    int recn = hparams.recn;
    int score = hparams.score;
    int frerecn_func = hparams.frerecn_func;
    long pre_size, rem_mem, rem_vdisks, rem_mem_old, rem_vdisks_old;

    num_vdisks = cache->num_vdisks;

    BitArray **agg;
    BitArray *ba[num_vdisks + 1];
    FILE *dfp[num_vdisks + 1];
    long *scores[num_vdisks + 1];
    double mem_limits[num_vdisks + 1];
    int obj_pos[num_vdisks + 1];

    agg = new BitArray *[num_vdisks + 1];

    fprintf(debug_file, "Heuristic used in analysing snapshots: ");
    switch (heuristic)
    {
        case K_FREQ:
            fprintf(debug_file, "k-Frequent (key: %d%%)\n", freq);
            break;
        case K_FREQ_MEM:
            fprintf(debug_file, "Constrained-k-Frequent\n");
            break;
        case K_RECN:
            fprintf(debug_file, "k-Recent (k: %d)\n", recn);
            break;
        case K_RECN_MEM:
            fprintf(debug_file, "Constrained-k-Recent\n");
            break;
        case K_FRERECN:
            fprintf(debug_file, "k-Frerecent (k: %d%%, func: %s)\n", score, frerecn_func ? "QUADRATIC" : "LINEAR");
            break;
        case K_FRERECN_MEM:
            fprintf(debug_file, "Constrained-k-Frerecent (func: %s)\n", frerecn_func ? "QUADRATIC" : "LINEAR");
            break;
        default:
            fprintf(debug_file, "unknown!\n");
            break;
    }

    if (experiment == EXP1 && heuristic == K_RECN)
    {
        i = recn - 1;
        last_ss = 0;
    }
    else
    {
        i = max_epochs - 1;
        //last_ss = max(0, i - MAX_LAST_SS);
        last_ss = 0;
    }

    for (; i >= last_ss; i--)
    {
        epoch = i;
        for (j = 0; j <= num_vdisks; j++)
        {
            if (!inmem)
            {
                if (j < num_vdisks)
                    snprintf(fname, MAX_FILENAME_LEN, "%s%s%d/%s/%s-%d/%d/%d.%d.%s", cache->dump_file_basename, "exp",
                             experiment, cache->wload_name, "VDISK", cache->vdisk_ids[j], ssr, cache->cache_idx, epoch,
                             cache->type_bitmap == BITARRAY ? "bin" : "csv");
                else
                    snprintf(fname, MAX_FILENAME_LEN, "%s%s%d/%s/%s/%d/%d.%d.%s", cache->dump_file_basename, "exp",
                             experiment, cache->wload_name, "HM3", ssr, cache->cache_idx,
                             epoch, cache->type_bitmap == BITARRAY ? "bin" : "csv");
                dfp[j] = fopen(fname, "rb");
                assert(dfp[j] != nullptr);
                ba[j] = new BitArray(dfp[j]);
                //fprintf(stderr, "%.2lf\n",
                //        (ba[j]->onbits * ((j == num_vdisks) ? SIZE_HM3_OBJ : SIZE_HM1_OBJ)) / (double) NUM_BYTES_IN_MB);
                fclose(dfp[j]);
            }
            else
                ba[j] = cache->snapshots[epoch][j]->get_bitmap();

            if (epoch == max_epochs - 1 || (heuristic == K_RECN && epoch == recn - 1))
            {
                agg[j] = new BitArray(ba[j]->bits);
                assert(agg[j] != nullptr);
                if (heuristic == K_FREQ && freq == -1)
                {
                    for (k = 0; k < agg[j]->size; k++)
                        agg[j]->bitmap[k] = UINT64_MAX;
                    agg[j]->onbits = agg[j]->size * 64;
                }
                else if (heuristic != K_RECN)
                {
                    scores[j] = new long[agg[j]->size * 64];
                    assert(scores[j] != nullptr);
                    memset(scores[j], 0, (agg[j]->size * 64) * sizeof(long));
                }
            }

            switch (heuristic)
            {
                case K_FREQ:
                    if (freq == -1)
                        agg[j]->_and(ba[j]);
                    else
                        calc_scores_freq(ba[j], nullptr, scores[j], 0);
                    break;
                case K_RECN:
                    if (experiment != EXP1 && i == max_epochs - recn)
                        i = 0;
                    agg[j]->_or(ba[j]);
                    break;
                case K_FRERECN:
                    calc_scores_frerecn(ba[j], nullptr, scores[j],
                                        (experiment == EXP1) ? pow(max_epochs - i - 1, frerecn_func + 1) :
                                        pow(i, frerecn_func + 1), 0);
                    break;

                case K_FREQ_MEM:
                    calc_scores_freq_mem(ba[j], scores[j]);
                    break;
                case K_RECN_MEM:
                    calc_scores_rec_mem(ba[j], scores[j], (experiment == EXP1) ? max_epochs - i - 1 : i + 1);
                    break;
                case K_FRERECN_MEM:
                    calc_scores_frerecn_mem(ba[j], scores[j],
                                            (experiment == EXP1) ? pow(max_epochs - i - 1, frerecn_func + 1) :
                                            pow(i + 1, frerecn_func + 1));
                    break;
                default:
                    fprintf(debug_file, "Unknown heuristic used!\n");
                    return nullptr;
            }

            delete ba[j];
        }
    }


    fprintf(debug_file, "Memory limit on the prewarm set size: %.2lf MB\n", pre_size_limit / 1048576.0);

    if (heuristic == K_FREQ && freq != -1)
    {
        if (freq < -1)
            freq = -max_epochs / freq;
        for (i = 0; i <= num_vdisks; i++)
        {
            calc_scores_freq(nullptr, agg[i], scores[i], freq);
            delete[] scores[i];
        }
    }
    else if (heuristic == K_FRERECN)
    {
        for (i = 0; i <= num_vdisks; i++)
        {
            calc_scores_frerecn(nullptr, agg[i], scores[i], 0, score);
            delete[] scores[i];
        }
    }
    else if (heuristic != K_FREQ && heuristic != K_RECN)
    {
        if (part_scheme_hm1 != HM1_NO_PART)
        {
            // Assume part_scheme_hm1_hm3 = EQUAL_HM3
            if (part_scheme_hm1 == HM1_FAIR_SHARE)
            {
                for (i = 0; i < num_vdisks; i++)
                    mem_limits[i] = (pre_size_limit / 2.0) / num_vdisks;
                mem_limits[i] = pre_size_limit / 2.0;
            }
            else if (part_scheme_hm1 == HM1_PROP_SHARE)
            {
                total_objs = 0;
                for (i = 0; i < num_vdisks; i++)
                    total_objs += cache->ba1[i]->onbits;
                for (i = 0; i < num_vdisks; i++)
                    mem_limits[i] = ((double) cache->ba1[i]->onbits / total_objs) * (pre_size_limit / 2.0);
                mem_limits[i] = pre_size_limit / 2.0;
            }

            rem_mem = rem_vdisks = 0;
            memset(obj_pos, 0, sizeof(int) * (num_vdisks + 1));
            for (i = 0; i < num_vdisks; i++)
            {
                k = analyze_scores(agg[i], scores[i], &obj_pos[i], SIZE_HM1_OBJ, mem_limits[i]);
                if (k < SIZE_HM1_OBJ)
                    rem_vdisks++;
                else
                {
                    mem_limits[i] = 0;
                    rem_mem += k;
                }
            }
            analyze_scores(agg[i], scores[i], &obj_pos[i], SIZE_HM3_OBJ, mem_limits[i]);

            while (part_scheme_hm1 == HM1_FAIR_SHARE && rem_mem > SIZE_HM1_OBJ)
            {
                //fprintf(stderr, "rem_mem: %lu, rem_vdisks: %lu\n", rem_mem, rem_vdisks);
                if (rem_mem >= rem_vdisks * SIZE_HM1_OBJ)
                {
                    rem_mem_old = rem_mem;
                    rem_vdisks_old = rem_vdisks;
                    rem_mem = rem_vdisks = 0;
                    for (i = 0; i < num_vdisks; i++)
                    {
                        if (mem_limits[i] == 0)
                            continue;
                        mem_limits[i] = (double) rem_mem_old / rem_vdisks_old;
                        k = analyze_scores(agg[i], scores[i], &obj_pos[i], SIZE_HM1_OBJ, mem_limits[i]);
                        if (k < SIZE_HM1_OBJ)
                            rem_vdisks++;
                        else
                        {
                            mem_limits[i] = 0;
                            rem_mem += k;
                        }
                    }
                }
                else
                {
                    for (i = 0; i < num_vdisks && rem_mem >= SIZE_HM1_OBJ; i++)
                    {
                        if (mem_limits[i] == 0)
                            continue;
                        analyze_scores(agg[i], scores[i], &obj_pos[i], SIZE_HM1_OBJ, SIZE_HM1_OBJ);
                        rem_mem -= SIZE_HM1_OBJ;
                    }
                }
            }
            while (part_scheme_hm1 == HM1_PROP_SHARE && rem_mem > SIZE_HM1_OBJ)
            {
                if (rem_mem >= rem_vdisks * SIZE_HM1_OBJ)
                {
                    total_objs = 0;
                    for (i = 0; i < num_vdisks; i++)
                    {
                        if (mem_limits[i] == 0)
                            continue;
                        total_objs += cache->ba1[i]->onbits;
                    }
                    for (i = 0; i < num_vdisks; i++)
                    {
                        if (mem_limits[i] == 0)
                            continue;
                        mem_limits[i] = ((double) cache->ba1[i]->onbits / total_objs) * rem_mem;
                    }
                    rem_mem = rem_vdisks = 0;
                    for (i = 0; i < num_vdisks; i++)
                    {
                        if (mem_limits[i] == 0)
                            continue;
                        k = analyze_scores(agg[i], scores[i], &obj_pos[i], SIZE_HM1_OBJ, mem_limits[i]);
                        if (k < SIZE_HM1_OBJ)
                            rem_vdisks++;
                        else
                        {
                            mem_limits[i] = 0;
                            rem_mem += k;
                        }
                    }
                }
                else
                {
                    for (i = 0; i < num_vdisks && rem_mem >= SIZE_HM1_OBJ; i++)
                    {
                        if (mem_limits[i] == 0)
                            continue;
                        analyze_scores(agg[i], scores[i], &obj_pos[i], SIZE_HM1_OBJ, SIZE_HM1_OBJ);
                        rem_mem -= SIZE_HM1_OBJ;
                    }
                }
            }
        }
        else //if(part_scheme_hm1 == HM1_NO_PART) i.e. no partitions at all
            analyze_scores_all(agg, scores, num_vdisks, pre_size_limit, ret_struct, ret_n, heuristic);

        for (i = 0; i <= num_vdisks; i++)
            delete[] scores[i];
    }

    onbits1 = 0;
    for (j = 0; j < num_vdisks; j++)
        onbits1 += agg[j]->onbits;
    onbits3 = agg[j]->onbits;
    pre_size = onbits1 * SIZE_HM1_OBJ + onbits3 * SIZE_HM3_OBJ;
    fprintf(debug_file, "Total prewarm set size after analysis: %0.2lf MB\n", pre_size / 1048576.0);

    if (pre_size > pre_size_limit)
    {
        fprintf(debug_file, "Prewarm set size exceeded memory limit! Aborting!\n");
        fprintf(stderr, "Prewarm set size exceeded memory limit! Aborting!\n");
        return nullptr;
    }
    fprintf(debug_file, "Needed to fetch %lu objects from the metadata store (%lu HM1 + %lu HM3)\n", onbits1 + onbits3,
            onbits1, onbits3);
    return agg;
}
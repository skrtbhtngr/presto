#pragma once

#include "util.h"
#include "hashmap.h"
#include "metadata.h"
#include "policy.h"
#include "bits.h"
#include "snapshot.h"
#include "workload.h"
#include "analyze.h"

enum ReplPolicy
{
    REPL_LRU,
    REPL_LFU
};

enum BitmapType
{
    BITARRAY,
    BITSET
};

class Cache
{
public:
    int cache_idx = -1;
    ReplPolicy policy_replace;
    BitmapType type_bitmap;
    bool singlepool;

    Pair *lru_list[NUM_POOLS][Q_PTRS];
    Pair *wss_list[Q_PTRS];
    //LFUList *lfu_list[NUM_POOLS];

    //unsigned char latency_hit; /* ms */
    //unsigned char latency_miss; /* ms */

    //unsigned long lru_counter[NUM_POOLS];
    int counter;

    int num_vdisks;
    int *vdisk_sizes;
    int *vdisk_ids;
    int *vtl;
    const char *wload_name;

    long mem_limit[NUM_POOLS];
    long total_cache_size;
    long mem_usage[NUM_POOLS];

    Hashmap *hm[NUM_HASHMAPS];
    int size_obj[NUM_HASHMAPS];

    long *hashmap_obj_count_vdisk[NUM_HASHMAPS];             // hashmaps X vdisks
    long *cache_obj_count_vdisk[NUM_POOLS][NUM_HASHMAPS];    // pools X hashmaps X vdisks
    long cache_obj_count[NUM_POOLS][NUM_HASHMAPS];           // pools X hashmaps
    long *hit_count_vdisk[NUM_POOLS][NUM_HASHMAPS];         // pools X hashmaps X vdisks
    long *miss_count_vdisk[NUM_HASHMAPS];                   // hashmaps X vdisks
    long *evct_count_vdisk_hm1[NUM_POOLS];                  // pools X vdisks
    long evct_count[NUM_POOLS][NUM_HASHMAPS];               // pools X hashmaps
    long *io_count_vdisk;                                   // vdisk

    BitSet **bs1;
    BitSet *bs3;

    BitArray **ba1;
    BitArray *ba3;
    BitArray **oba1;
    BitArray *oba3;

    const char *dump_file_basename = PRESTO_BASEPATH "dumps/";
    void *comp_mem;
    Snapshot ***snapshots;

    int run_ssr;
    int run_max_epochs;

    Cache()
    {}

    Cache(int idx, Hashmap *hm1, Hashmap *hm3, Workload *wload, int size_MB)
    {
        int i, j, k;
        this->counter = 0;
        this->cache_idx = idx;
        this->hm[HASHMAP1] = hm1;
        this->hm[HASHMAP3] = hm3;
        this->policy_replace = REPL_LRU;
        this->type_bitmap = BITARRAY;
        this->mem_limit[POOL_SINGLE] = (long) (0.2 * (size_MB * (long) NUM_BYTES_IN_MB));   // 20% of cache
        this->mem_limit[POOL_MULTI] = (long) (0.8 * (size_MB * (long) NUM_BYTES_IN_MB));    // 80% of cache
        this->total_cache_size = size_MB * (long) NUM_BYTES_IN_MB;
        this->size_obj[HASHMAP1] = SIZE_HM1_OBJ;
        this->size_obj[HASHMAP3] = SIZE_HM3_OBJ;
        this->num_vdisks = wload->num_vdisks;
        this->wload_name = wload->name;
        this->singlepool = false;

        this->io_count_vdisk = new long[num_vdisks];
        memset(this->io_count_vdisk, 0, num_vdisks * sizeof(long));
        for (i = 0; i < NUM_HASHMAPS; i++)
        {
            this->hashmap_obj_count_vdisk[i] = new long[num_vdisks];
            memset(this->hashmap_obj_count_vdisk[i], 0, num_vdisks * sizeof(int));

            this->miss_count_vdisk[i] = new long[num_vdisks];
            memset(this->miss_count_vdisk[i], 0, num_vdisks * sizeof(long));
        }
        for (i = 0; i < NUM_POOLS; i++)
        {
            this->evct_count_vdisk_hm1[i] = new long[num_vdisks];
            memset(this->evct_count_vdisk_hm1[i], 0, num_vdisks * sizeof(unsigned long));

            for (j = 0; j < NUM_HASHMAPS; j++)
            {
                this->cache_obj_count_vdisk[i][j] = new long[num_vdisks];
                memset(this->cache_obj_count_vdisk[i][j], 0, num_vdisks * sizeof(int));

                this->hit_count_vdisk[i][j] = new long[num_vdisks];
                memset(this->hit_count_vdisk[i][j], 0, num_vdisks * sizeof(long));
            }
        }
        memset(this->evct_count, 0, sizeof(evct_count));
        this->vdisk_sizes = new int[num_vdisks];
        this->vdisk_ids = new int[num_vdisks];
        this->vtl = new int[1000];
        for (i = 0; i < num_vdisks; i++)
        {
            this->vdisk_ids[i] = wload->vdisks[i].vdisk_id;
            this->vdisk_sizes[i] = wload->vdisks[i].size_gb;
            this->vtl[this->vdisk_ids[i]] = i;
        }
        if (type_bitmap == BITARRAY)
        {
            ba1 = new BitArray *[num_vdisks + 1];
            oba1 = new BitArray *[num_vdisks + 1];
            for (i = 0; i < num_vdisks; i++)
            {
                ba1[i] = new BitArray(vdisk_sizes[i] * (NUM_BYTES_IN_GB / NUM_BYTES_IN_EXTENT));
                oba1[i] = new BitArray(vdisk_sizes[i] * (NUM_BYTES_IN_GB / NUM_BYTES_IN_EXTENT));
            }
            ba3 = ba1[i] = new BitArray(1UL << HASH_CHARS_FOR_EGROUP * 4);
            oba3 = oba1[i] = new BitArray(1UL << HASH_CHARS_FOR_EGROUP * 4);
        }
        //else if (type_bitmap == BITSET)
        //{
        //    bs1 = new BitSet *[num_vdisks];
        //    for (i = 0; i < num_vdisks; i++)
        //        bs1[i] = new BitSet(&comp_bitset, &print_bitset, &save_bitset);
        //    bs3 = new BitSet(&comp_bitset, &print_bitset, &save_bitset);
        //}

        // allocate 50 MB in system pages
        //comp_mem = mmap(NULL, NUM_BYTES_IN_MB * 50, PROT_READ | PROT_WRITE,
        //                MAP_SHARED | MAP_ANON, -1, 0);
        //assert(comp_mem != MAP_FAILED);

        snapshots = new Snapshot **[MAX_SNAPSHOTS];
    }

    ~Cache()
    {
        int i, j;
        for (i = 0; i < NUM_POOLS; i++)
        {
            for (j = 0; j < NUM_HASHMAPS; j++)
            {
                delete[] cache_obj_count_vdisk[i][j];
                delete[] hit_count_vdisk[i][j];
            }
        }
        for (i = 0; i < NUM_HASHMAPS; i++)
        {

            delete[] miss_count_vdisk[i];
            delete[] hashmap_obj_count_vdisk[i];
        }
        delete[] vdisk_sizes;
        if (type_bitmap == BITARRAY)
        {
            for (i = 0; i < num_vdisks; i++)
            {
                delete ba1[i];
                delete oba1[i];
            }
            delete[] ba1;
            delete[] oba1;
            delete ba3;
            delete oba3;
        }
        //if (munmap(comp_mem, NUM_BYTES_IN_MB * 50) == -1)
        //{
        //    fprintf(stderr, "munmap() failed!\n");
        //    return;
        //}
        delete[] snapshots;
    }

    void *lookup(HMType type, void *k, int vdisk);

    void fetch_into_single(Pair *val);

    void single_to_multi(Pair *val);

    void fetch_into_multi(Pair *val);

    void touch_pool(Pair *val, int pool);

    long snapshot_bitmaps(int epoch, int ssr, bool inmem, int experiment);

    long delta_bits();

    void dump_vdisk(int vdisk, FILE *fp);

    int load_dump(BitArray *ba, int vdisk, HMType type, int prewarm_set_share);

    void reset();

    //void reset_vdisk(unsigned int vdisk);

    void print_cache_stats(FILE *file, bool full);

    double get_hit_ratio();

    double get_hit_ratio(int vdisk);

    void copy(Cache *c);

    long load_dump_nopart(struct kv_analyze_all *kvs, long n, long prewarm_set_limit, long *pos,
                          int *prewarm_set_used_size, int *prewarm_set_used_objs);

    long *estimate_wss(int window, bool full);

    long order_mig_ba(BitArray *ba, void **ret_struct, long *ret_n, int heuristic, int vdisk);

    long load_vdisk_ba(struct kv_analyze_type *kvs, long n, long prewarm_set_limit, long *pos, int *prewarm_set_used_size,
                       int *prewarm_set_used_objs, int vdisk);

};

void clean(void *node, void *fp);

void clean_vdisk(void *data, void *vdisk);

void print_facts(FILE *file, int exp, Cache *cache);
#pragma once

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <climits>
#include <cstdbool>
#include <cstdint>
#include <cinttypes>
#include <cassert>
#include <cctype>
#include <cmath>
#include <semaphore.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <zlib.h>

#include <algorithm>
#include <new>
#include <string>
//#include "cache.h"

using namespace std;

#define min(a, b) ((a) < (b) ? (a): (b))
#define SHA_DIGEST_HEX_LENGTH 40
#define MAX_REQ_LENGTH 100
#define NUM_BYTES_IN_MB 1048576U
#define NUM_BYTES_IN_EXTENT 1048576U
#define NUM_BYTES_IN_GB 1073741824U
#define TB_TO_UINT64 16384
#define GB_TO_UINT64 16
#define B_TO_UINT64 67108864

#define NUM_CACHES 2
#define CACHE_A 0
#define CACHE_B 1


#define MAX_VDISK_SIZE (2U*(1U<<20U)) // in vblocks, not bytes!
#define B_TO_UINT64 67108864
#define MAX_FILENAME_LEN 200

#define NUM_POOLS 2
#define POOL_SINGLE 0
#define POOL_MULTI 1

#define Q_PTRS 2
#define FRONT 0
#define REAR 1

#define IN_SINGLE  0x01U
#define IN_MULTI   0x02U

#define NUM_SLICES_IN_EGROUP 128
#define NUM_EXTENTS_IN_EGROUP 4

#define NUM_HASHMAPS 3

#define SHA_DIGEST_HEX_LENGTH 40
#define HASH_CHARS_FOR_EGROUP 7U

#define MEM_LIMIT_POOL_SINGLE (200 * NUM_BYTES_IN_MB)
#define MEM_LIMIT_POOL_MULTI  (800 * NUM_BYTES_IN_MB)

#define SIZE_HM1_OBJ 1400
#define SIZE_HM3_OBJ 27648

//#define SIZE_HM1_OBJ 731
//#define SIZE_HM3_OBJ 28174

#define REQ_Q_LEN 20000
#define TOTAL_REQS 195000000
#define MAX_SNAPSHOTS 1000
#define MAX_TIME_SEC 14400

#define HEURISTIC_FREQ_MIN_VAL 2
#define HEURISTIC_RECN_MIN_VAL 100
#define MAX_LAST_SS 200

#define PRESTO_BASEPATH "/home/skrtbhtngr/CLionProjects/presto/"

#define LEN(x, y) ((signed) ((sizeof(x))/(sizeof(y))))

#define EXP1 1U
#define EXP2 2U
#define EXP3 3U
#define EXP4 4U

struct bio_req
{
    int vdisk;
    int size; /* in bytes */
    //long offset;
    /* unsigned char *data; we don't care */

    int vblock;
    int vblock_offset;
    double ts;
    //char type;
};

char *hashit(unsigned int vdisk, unsigned long vblock);

char *int64_to_binary(unsigned long dec);

void line(int n, FILE *file);

bool file_exists(const char *fname);


enum Role
{
    SOURCE,
    DESTINATION
};

enum Heuristic
{
    K_FREQ,
    K_FREQ_MEM,
    K_RECN,
    K_RECN_MEM,
    K_FRERECN,
    K_FRERECN_MEM
};

enum FrerecnFunction
{
    LINEAR,
    QUADRATIC
};

enum PartitionSchemeHM1
{
    HM1_PROP_SHARE,
    HM1_FAIR_SHARE,
    HM1_NO_PART
};

enum PartitionSchemeHM1HM3
{
    EQUAL_HM3,
    NO_HM3,
    CLOSED_HM3,
    HM1_HM3_NO_PART
};
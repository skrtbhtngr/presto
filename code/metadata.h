#pragma once

#include "util.h"

struct extent_id
{
    long vblock;
    int vdisk;
    //unsigned char egoup_mapping_in_eid_map;
};

//struct region
//{
//    unsigned long block_offset;
//    unsigned long length;
//    extent_id *eid;
//    unsigned long extent_group_id;
//};

struct key_hm1
{
    extent_id eid;
    char hash[41];
};

//struct val_hm1
//{
//    unsigned long num_regions;
//    struct region regions[];
//};

struct val_hm1
{
    //extent_id eid;
    long egroup_id;
};

struct key_hm3
{
    long egroup_id;
};

struct extent_state
{
    extent_id eid;
//    unsigned long first_slice_offset;
//    unsigned long refcount;
};

struct slice_state
{
//    unsigned long slice_id;
//    unsigned long extent_group_offset;
    unsigned int extent_group_offset;
};

struct val_hm3
{
    unsigned char num_extents;
    //slice_state slices[NUM_SLICES_IN_EGROUP];
    extent_state extents[NUM_EXTENTS_IN_EGROUP];
};

#define to_hm1key(x) ((struct key_hm1 *) (x)->key)
#define to_hm1val(x) ((struct val_hm1 *) (x)->val)
#define to_hm3key(x) ((struct key_hm3 *) (x)->key)
#define to_hm3val(x) ((struct val_hm3 *) (x)->val)

int comp_hm1(void *a, void *b);

key_hm1 *get_hm1_key(int vdisk, long vblock, const char *hash);

val_hm1 *get_hm1_val(int vdisk, long vblock, const char *hash);

int comp_hm3(void *a, void *b);

key_hm3 *get_hm3_key(long egroup_id);

val_hm3 *get_hm3_val(long egroup_id, int vdisk, long vblock,
                     long vblock_offset, long size);

void update_hm3_val(val_hm3 *vhm, int egroup_id, int vdisk,
                    long vblock, long vblock_offset, long size);

int comp_bitset(void *a, void *b);

void print_bitset(void *data, void *fp);

void save_bitset(void *data, void *fp);

#pragma once

#include "util.h"
#include "hashmap.h"

void insert_lru_list(int c, Pair *list[], Pair *val, int ts);

void delete_lru_list(int c, Pair *list[], Pair *val);

Pair *evict_lru_list(int c, Pair *list[]);

//void insert_lfu_list(int c, LFUList **list, Pair *val);

//void update_lfu_list(int c, LFUList **list, Pair *val);

//void delete_lfu_list(int c, LFUList **list, Pair *val);

//Pair *evict_lfu_list(int c, LFUList **list);

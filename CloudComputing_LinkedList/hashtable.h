#pragma once
#include "list.h"

// num buckets
#define NUM_HASH_TABLE 131072

// visit all hashtable entries
#define hashtable_for_each_safe(pos, n, ht) \
	for(int i =0; i<NUM_HASH_TABLE; i++) \
		list_for_each_safe(pos, n, &(ht->list[i]))

struct HASH_TABLE_T {
	struct LIST_HEAD_T list[NUM_HASH_TABLE];
};

int get_hash(int value);

void hashtable_init(struct HASH_TABLE_T *ht);

int hashtable_put(struct HASH_TABLE_T *ht, int hash, struct LIST_HEAD_T *link);

struct LIST_HEAD_T *hashtable_get(struct HASH_TABLE_T *ht, int hash);

int hashtable_del(struct HASH_TABLE_T *ht, int hash, struct LISH_HEAD_T *link);
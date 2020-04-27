#include "hashtable.h"
#include <stdlib.h>


int get_hash(int value) {
	return value % NUM_HASH_TABLE;
}

void hashtable_init(struct HASH_TABLE_T *ht) {
	for (int i = 0; i < NUM_HASH_TABLE; i++) {
		list_init(&ht->list[i]);
	}
}

int hashtable_put(struct HASH_TABLE_T *ht, int hash, struct LIST_HEAD_T *link) {
	list_append(&ht->list[hash], link);
}

struct LIST_HEAD_T *hashtable_get(struct HASH_TABLE_T *ht, int hash) {
	return &ht->list[hash];
}

int hashtable_del(struct HASH_TABLE_T *ht, int hash, struct LIST_HEAD_T *link) {
	struct LIST_HEAD_T *p_node, *tmp;
	list_for_each_safe(p_node, tmp, &ht->list[hash]) {
		if (p_node == link) {
			list_remove(p_node);
			return 1;
		}
	}
	return 0;
}
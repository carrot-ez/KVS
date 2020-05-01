#pragma once

#define SKIPLIST_MAX_LEVEL  16

#define skiplist_for_each_safe(pos, n, head) \
	for(pos = (head)->forward[1], n = pos->forward[1]; \
		pos != (head); pos = n, n = pos->forward[1])

typedef struct snode {
    int key;
    int value;
    struct snode** forward;
} snode;

typedef struct skiplist {
    int level;
    int size;
    struct snode* header;
} skiplist;

skiplist *skiplist_init(skiplist *list);

static int rand_level();

int skiplist_insert(skiplist* list, snode *node);

snode* skiplist_search(skiplist* list, int key);

static void skiplist_node_free(snode* x);

int skiplist_delete(skiplist* list, int key);

int skiplist_clear(skiplist *list);
#pragma once
#include "list.h"
#include "hashtable.h"
#include "skiplist.h"

#include <pthread.h>

// you should select one of these options
#define LOCK_MODE_MUTEX 0 // using mtex lock
#define LOCK_MODE_RW 1 // using rwlock

// you should select one of these options
#define USING_LIST 0 // using linked list
#define USING_HASH_TABLE 0 // using hashtable
#define USING_SKIP_LIST 1 // using skip list

// for code refactoring
#define KVS_UNLOCK(kvs, lock)	\
	if(LOCK_MODE_MUTEX) pthread_mutex_unlock(kvs->lock);	\
	else if(LOCK_MODE_RW) pthread_rwlock_unlock(kvs->lock)

typedef struct KVS_T {
#if LOCK_MODE_MUTEX
	pthread_mutex_t lock;
#elif LOCK_MODE_RW
	pthread_rwlock_t lock;
#endif

#if USING_HASH_TABLE 
	struct HASH_TABLE_T ht;
#elif USING_LIST
	LIST_HEAD_T list;
#elif USING_SKIP_LIST
	struct skiplist list;
#endif
	
	int size; // 삽입 삭제 후 KVS안의 원소수 비교를 위해 추가
}KVS_T;

#if USING_SKIP_LIST
typedef struct snode
#else
typedef struct KVS_NODE_T {
	int key;
	int value;
	LIST_HEAD_T list;
}
#endif
KVS_NODE_T;

KVS_T *kv_create_db();
int kv_destroy_db(KVS_T *kvs);

int kv_put(KVS_T *kvs, KVS_NODE_T *kv_pair);
KVS_NODE_T *kv_get(KVS_T *kvs, int key);
int kv_delete(KVS_T *kvs, int key);
KVS_NODE_T *kv_get_range(KVS_T *kvs, int start_key, int end_key, int *num_entries);

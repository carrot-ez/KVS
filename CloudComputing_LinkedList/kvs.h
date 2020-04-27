#pragma once
#include "list.h"
#include "hashtable.h"
#include <pthread.h>

// you should select one of these options
#define LOCK_MODE_MUTEX 0 // using mtex lock
#define LOCK_MODE_RW 1 // using rwlock

// you should select one of these options
#define USING_HASH_TABLE 1 // using hashtable
#define USING_LIST 0 // using linked list

typedef struct KVS_T {
#if LOCK_MODE_MUTEX
	pthread_mutex_t lock;
#elif LOCK_MODE_RW
	pthread_rwlock_t lock;
#endif

#if USING_HASH_TABLE 
	struct HASH_TABLE_T ht;
#else USING_LIST
	LIST_HEAD_T list;
#endif
	
	int size; // 삽입 삭제 후 KVS안의 원소수 비교를 위해 추가
}KVS_T;

typedef struct KVS_NODE_T {
	int key;
	int value;
	LIST_HEAD_T list;
}KVS_NODE_T;

KVS_T *kv_create_db();
int kv_destroy_db(KVS_T *kvs);

int kv_put(KVS_T *kvs, KVS_NODE_T *kv_pair);
KVS_NODE_T *kv_get(KVS_T *kvs, int key);
int kv_delete(KVS_T *kvs, int key);
#if USING_LIST
KVS_NODE_T *kv_get_range(KVS_T *kvs, int start_key, int end_key, int *num_entries);
#endif
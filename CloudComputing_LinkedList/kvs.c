#include "kvs.h"
#include<stdlib.h>

/**
* KVS_T 객체를 초기화하고 반환한다.
*/
KVS_T *kv_create_db() {
	KVS_T *kvs = (KVS_T *)malloc(sizeof(KVS_T));
	if (kvs == NULL) {
		printf("create error\n");
		return NULL;
	}

	// KVS생성과 함께 락변수 초기화
#if LOCK_MODE_MUTEX
	int ret = pthread_mutex_init(&kvs->lock, NULL);
#elif LOCK_MODE_RW
	int ret = pthread_rwlock_init(&kvs->lock, NULL);
#endif
	if (ret != 0) {
		printf("pthread lock init error\n");
		return NULL;
	}

#if USING_HASH_TABLE
	hashtable_init(&kvs->ht);
#elif USING_LIST
	list_init(&kvs->list);
#endif

	// KVS의 size를 알기위해 하나의 변수 추가
	kvs->size = 0;

	return kvs;
}

/**
* @kvs	: KVS
* kvs의 모든 노드를 메모리 해제하고, kvs도 메모리 해제한다.
*/
int kv_destroy_db(KVS_T *kvs) {
	if (kvs == NULL) {
		printf("destroy error\n");
		return -1;
	}

	// kvs의 락변수 해제
#if LOCK_MODE_MUTEX
	pthread_mutex_destroy(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_destroy(&kvs->lock);
#endif

	LIST_HEAD_T *p_node, *tmp;
	KVS_NODE_T *k_node;

#if USING_HASH_TABLE
	hashtable_for_each_safe(p_node, tmp, (&kvs->ht))
#elif USING_LIST
	list_for_each_safe(p_node, tmp, &kvs->list) 
#endif
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		list_remove(&k_node->list);
		free(k_node);
	}

	free(kvs);
	return 1;
}

/**
* @kvs		: KVS
* @kv_pair	: kvs에 삽입할 노드
* 성공시 1, 덮어쓰기시 0, 에러시 -1을 반환한다.
*/
int kv_put(KVS_T *kvs, KVS_NODE_T *kv_pair) {
	if (kvs == NULL) {
		printf("kvs is null\n");
		return -1;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node = NULL;
	// hashtable을 사용할 경우 hash 구하기
#if USING_HASH_TABLE
	int hash = get_hash(kv_pair->key);
#endif

	// 삽입 전 lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_wrlock(&kvs->lock);
#endif

#if USING_LIST
	list_for_each(p_node, &kvs->list) 
#elif USING_HASH_TABLE
	list_for_each(p_node, hashtable_get(&kvs->ht, hash)) 
#endif
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if (k_node->key == kv_pair->key) {
			k_node->value = kv_pair->value;

			// 반환 전 lock 해제
#if LOCK_MODE_MUTEX
			pthread_mutex_unlock(&kvs->lock);
#elif LOCK_MODE_RW
			pthread_rwlock_unlock(&kvs->lock);
#endif

			return 0;
		}
	}

	k_node = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	// 메모리 할당 실패시
	if (k_node == NULL) {
		printf("malloc error\n");

		// unlock
#if LOCK_MODE_MUTEX
		pthread_mutex_unlock(&kvs->lock);
#elif LOCK_MODE_RW
		pthread_rwlock_unlock(&kvs->lock);
#endif

		return -1;
	}

	k_node->key = kv_pair->key;
	k_node->value = kv_pair->value;
	list_init(&k_node->list);

#if USING_HASH_TABLE
	hashtable_put(&kvs->ht, hash, &k_node->list);
#elif USING_LIST
	list_append(&kvs->list, &k_node->list);
#endif

	kvs->size += 1;

	// unlock
#if LOCK_MODE_MUTEX
	pthread_mutex_unlock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_unlock(&kvs->lock);
#endif

	return 1;
}

/**
* @kvs	: KVS
* @key	: kvs에서 검색을 수행할 키
* 검색 성공시 key에 해당하는 노드 KVS_NODE_T를 반환
* 실패시 NULL을 반환
*/
KVS_NODE_T *kv_get(KVS_T *kvs, int key) {
	if (kvs == NULL) {
		printf("kvs is null\n");
		return NULL;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node, *ret;

	// 해시테이블 사용시 hash값 구하기
#if USING_HASH_TABLE
	int hash = get_hash(key);
#endif

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_rdlock(&kvs->lock);
#endif

#if USING_LIST
	list_for_each(p_node, &kvs->list) 
#elif USING_HASH_TABLE
	list_for_each(p_node, hashtable_get(&kvs->ht, hash))
#endif
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if (k_node->key == key) {
			// memcpy는 node의 list까지 복사하므로 문제가 될 가능성이 있다고 판단했다.
			// 따라서 key, value값을 복사한 후, 자신만을 가르키는 node로 바꾼 후 반환하였다.
			ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
			if (ret == NULL) { // 메모리 할당 실패
				printf("malloc error\n");

				// unlock
#if LOCK_MODE_MUTEX
				pthread_mutex_unlock(&kvs->lock);
#elif LOCK_MODE_RW
				pthread_rwlock_unlock(&kvs->lock);
#endif

				return NULL;
			}
			ret->key = k_node->key;
			ret->value = k_node->value;
			list_init(&ret->list);

			// unlock
#if LOCK_MODE_MUTEX
			pthread_mutex_unlock(&kvs->lock);
#elif LOCK_MODE_RW
			pthread_rwlock_unlock(&kvs->lock);
#endif

			return ret;
		}
	}

	// unlock
#if LOCK_MODE_MUTEX
	pthread_mutex_unlock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_unlock(&kvs->lock);
#endif

	return NULL;
}


/**
* @kvs			: KVS
* @start_key	: 검색의 시작점을 나타내는 키
* @end_key		: 검색의 마지막점을 나타내는 키
* @num_entries	: 검색한 개수를 저장하기 위한 변수
* 검색 성공시 질의에 해당하는 노드들을 가진 리스트 KVS_NODE_T를 반환,
* 검색 실패시 NULL을 반환
*/
#if USING_LIST // list를 사용할때만 사용
KVS_NODE_T *kv_get_range(KVS_T *kvs, int start_key, int end_key, int *num_entries) {
	if (kvs == NULL) {
		printf("kvs is null\n");
		return NULL;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node, *tmp, *ret;

	ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	if (ret == NULL) { // 메모리 할당 실패시
		printf("malloc error\n");
		return NULL;
	}
	list_init(&ret->list);

	// 노드에 접근 전 lock
	pthread_mutex_lock(&kvs->lock);

	*num_entries = 0;
	list_for_each(p_node, &kvs->list) {
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if ((k_node->key >= start_key) && (k_node->key <= end_key)) {
			tmp = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
			if (tmp == NULL) { // 메모리 할당 실패
				printf("malloc error\n");

				// 함수 종료 전 lock 해제
				pthread_mutex_unlock(&kvs->lock);

				return NULL;
			}
			tmp->key = k_node->key;
			tmp->value = k_node->value;

			list_append(&ret->list, &tmp->list);
			*num_entries += 1;
		}
	}

	if (*num_entries == 0) { // 찾은 결과가 없을 떄
		free(ret);

		// 함수 종료 전 lock 해제
		pthread_mutex_unlock(&kvs->lock);

		return NULL;
	}

	// 함수 종료 전 lock 해제
	pthread_mutex_unlock(&kvs->lock);

	return ret;
}
#endif

/**
* @kvs	: key, value를 저장하는 KVS_T 구조체
* @key	: kvs에서 삭제할 key
* 삭제 성공시 1, 실패시 0, 에러 발생시 -1을 리턴한다.
*/
int kv_delete(KVS_T *kvs, int key) {
	if (kvs == NULL)
		return -1;
	LIST_HEAD_T *p_node, *tmp;
	KVS_NODE_T *k_node;
#if USING_HASH_TABLE
	int hash = get_hash(key);
#endif

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_wrlock(&kvs->lock);
#endif

#if USING_LIST
	list_for_each_safe(p_node, tmp, &kvs->list) 
#elif USING_HASH_TABLE
	list_for_each_safe(p_node, tmp, hashtable_get(&kvs->ht, hash))
#endif
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if (k_node->key == key) {
			list_remove(&k_node->list);
			free(k_node);
			kvs->size -= 1;

			// unlock
#if LOCK_MODE_MUTEX
			pthread_mutex_unlock(&kvs->lock);
#elif LOCK_MODE_RW
			pthread_rwlock_unlock(&kvs->lock);
#endif

			return 1;
		}
	}

	// unlock
#if LOCK_MODE_MUTEX
	pthread_mutex_unlock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_unlock(&kvs->lock);
#endif

	return 0;
}
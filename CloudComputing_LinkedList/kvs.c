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
#elif USING_SKIP_LIST
	skiplist_init(&kvs->list);
#endif

	// KVS의 size를 알기위해 하나의 변수 추가
	kvs->size = 0;

	return kvs;
}

int kv_destroy_db(KVS_T *kvs) {
#if USING_LIST
	if (kvs == NULL) {
		printf("destroy error\n");
		return -1;
	}
	KVS_NODE_T *k_node;

	// kvs의 락변수 해제
#if LOCK_MODE_MUTEX
	pthread_mutex_destroy(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_destroy(&kvs->lock);
#endif

	LIST_HEAD_T *p_node, *tmp;
	list_for_each_safe(p_node, tmp, &kvs->list)
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		list_remove(&k_node->list);
		free(k_node);
	}

	free(kvs);
	return 1;
	////////////////////////////////////////////
#elif USING_HASH_TABLE
	if (kvs == NULL) {
		printf("destroy error\n");
		return -1;
	}
	KVS_NODE_T *k_node;

	// kvs의 락변수 해제
#if LOCK_MODE_MUTEX
	pthread_mutex_destroy(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_destroy(&kvs->lock);
#endif

	LIST_HEAD_T *p_node, *tmp;
	hashtable_for_each_safe(p_node, tmp, (&kvs->ht))
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		list_remove(&k_node->list);
		free(k_node);
	}

	free(kvs);
	return 1;
	////////////////////////////////////
#elif USING_SKIP_LIST
	if (kvs == NULL) {
		printf("destroy error\n");
		return -1;
	}
	KVS_NODE_T *k_node;

	// kvs의 락변수 해제
#if LOCK_MODE_MUTEX
	pthread_mutex_destroy(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_destroy(&kvs->lock);
#endif

	skiplist_free(&kvs->list);

	free(kvs);
	return 1;
#endif
}

/**
* @kvs		: KVS
* @kv_pair	: kvs에 삽입할 노드
* 성공시 1, 덮어쓰기시 0, 에러시 -1을 반환한다.
*/
int kv_put(KVS_T *kvs, KVS_NODE_T *kv_pair) {
#if USING_LIST
	if (kvs == NULL) {
		printf("kvs is null\n");
		return -1;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node = NULL;

	// 삽입 전 lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_wrlock(&kvs->lock);
#endif
	list_for_each(p_node, &kvs->list)
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if (k_node->key == kv_pair->key) {
			k_node->value = kv_pair->value;

			// 반환 전 lock 해제
			KVS_UNLOCK(&kvs, lock);

			return 0;
		}
	}

	k_node = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	// 메모리 할당 실패시
	if (k_node == NULL) {
		printf("malloc error\n");

		// unlock
		KVS_UNLOCK(&kvs, lock);

		return -1;
	}

	k_node->key = kv_pair->key;
	k_node->value = kv_pair->value;
	//list_init(&k_node->list);

	list_append(&kvs->list, &k_node->list);
	kvs->size += 1;

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return 1;
	//////////////////////////////////////////////////////
#elif USING_HASH_TABLE
	if (kvs == NULL) {
		printf("kvs is null\n");
		return -1;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node = NULL;

	// hashtable을 사용할 경우 hash 구하기
	int hash = get_hash(kv_pair->key);

	// 삽입 전 lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_wrlock(&kvs->lock);
#endif

	list_for_each(p_node, hashtable_get(&kvs->ht, hash))
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if (k_node->key == kv_pair->key) {
			k_node->value = kv_pair->value;

			// 반환 전 lock 해제
			KVS_UNLOCK(&kvs, lock);

			return 0;
		}
	}

	k_node = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	// 메모리 할당 실패시
	if (k_node == NULL) {
		printf("malloc error\n");

		// unlock
		KVS_UNLOCK(&kvs, lock);

		return -1;
	}

	k_node->key = kv_pair->key;
	k_node->value = kv_pair->value;
	//list_init(&k_node->list);

	hashtable_put(&kvs->ht, hash, &k_node->list);

	kvs->size += 1;

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return 1;
	////////////////////////////////////////////////////////
#elif USING_SKIP_LIST
	if (kvs == NULL) {
		printf("kvs is null\n");
		return -1;
	}
	KVS_NODE_T *k_node = NULL;

	// 삽입 전 lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_wrlock(&kvs->lock);
#endif

	skiplist_insert(&kvs->list, kv_pair);
	kvs->size += 1;

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return 1;
#endif
}

/**
* @kvs	: KVS
* @key	: kvs에서 검색을 수행할 키
* 검색 성공시 key에 해당하는 노드 KVS_NODE_T를 반환
* 실패시 NULL을 반환
*/
KVS_NODE_T *kv_get(KVS_T *kvs, int key) {
#if USING_LIST
	if (kvs == NULL) {
		printf("kvs is null\n");
		return NULL;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node, *ret;

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_rdlock(&kvs->lock);
#endif

	list_for_each(p_node, &kvs->list)
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if (k_node->key == key) {
			// memcpy는 node의 list까지 복사하므로 문제가 될 가능성이 있다고 판단했다.
			// 따라서 key, value값을 복사한 후, 자신만을 가르키는 node로 바꾼 후 반환하였다.
			ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
			if (ret == NULL) { // 메모리 할당 실패
				printf("malloc error\n");

				// unlock
				KVS_UNLOCK(&kvs, lock);

				return NULL;
			}
			ret->key = k_node->key;
			ret->value = k_node->value;
			list_init(&ret->list);

			// unlock
			KVS_UNLOCK(&kvs, lock);

			return ret;
		}
	}

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return NULL;
#elif USING_HASH_TABLE
	if (kvs == NULL) {
		printf("kvs is null\n");
		return NULL;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node, *ret;

	// 해시테이블 사용시 hash값 구하기
	int hash = get_hash(key);

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_rdlock(&kvs->lock);
#endif

	list_for_each(p_node, hashtable_get(&kvs->ht, hash))
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if (k_node->key == key) {
			// memcpy는 node의 list까지 복사하므로 문제가 될 가능성이 있다고 판단했다.
			// 따라서 key, value값을 복사한 후, 자신만을 가르키는 node로 바꾼 후 반환하였다.
			ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
			if (ret == NULL) { // 메모리 할당 실패
				printf("malloc error\n");

				// unlock
				KVS_UNLOCK(&kvs, lock);

				return NULL;
			}
			ret->key = k_node->key;
			ret->value = k_node->value;
			list_init(&ret->list);

			// unlock
			KVS_UNLOCK(&kvs, lock);

			return ret;
		}
	}

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return NULL;
#elif USING_SKIP_LIST
	if (kvs == NULL) {
		printf("kvs is null\n");
		return NULL;
	}

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_rdlock(&kvs->lock);
#endif

	KVS_NODE_T *p_node, *ret;
	p_node = skiplist_search(&kvs->list, key);
	if (p_node) {
		ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
		ret->key = p_node->key;
		ret->value = p_node->value;

		// unlock
		KVS_UNLOCK(&kvs, lock);

		return ret;
	}

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return NULL;
#endif
}

/**
* @kvs			: KVS
* @start_key	: 검색의 시작점을 나타내는 키
* @end_key		: 검색의 마지막점을 나타내는 키
* @num_entries	: 검색한 개수를 저장하기 위한 변수
* 검색 성공시 질의에 해당하는 노드들을 가진 리스트 KVS_NODE_T를 반환,
* 검색 실패시 NULL을 반환
*/
KVS_NODE_T *kv_get_range(KVS_T *kvs, int start_key, int end_key, int *num_entries) {
#if USING_LIST // list를 사용할때만 사용
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

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#else
	pthread_rwlock_rdlock(&kvs->lock);
#endif

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

		// unlock
		KVS_UNLOCK(&kvs, lock);

		return NULL;
	}

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return ret;
#elif USING_HASH_TABLE
	if (kvs == NULL) {
		printf("kvs is null\n");
		return NULL;
	}
	LIST_HEAD_T *p_node, *n;
	KVS_NODE_T *k_node, *tmp, *ret;

	ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	list_init(&ret->list);

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#else 
	pthread_rwlock_rdlock(&kvs->lock);
#endif

	*num_entries = 0;
	hashtable_for_each_safe(p_node, n, (&kvs->ht)) {
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if ((k_node->key >= start_key) && (k_node->key <= end_key)) {
			tmp = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
			tmp->key = k_node->key;
			tmp->value = k_node->value;

			list_append(&ret->list, &tmp->list);
			*num_entries += 1;
		}
	}

	if (*num_entries == 0) { // 찾은 결과가 없을 떄
		free(ret);

		// unlock
		KVS_UNLOCK(&kvs, lock);

		return NULL;
	}

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return ret;
	////////////////////////////////////////////
#elif USING_SKIP_LIST
	if (kvs == NULL) {
		printf("kvs is null\n");
		return NULL;
	}

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#else
	pthread_rwlock_rdlock(&kvs->lock);
#endif

	KVS_NODE_T *ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	ret->forward = (KVS_NODE_T **)malloc(sizeof(KVS_NODE_T *) * 2);

	KVS_NODE_T *tmp = ret;

	KVS_NODE_T *x = (&kvs->list)->header;
	*num_entries = 0;
	int i;
	for (i = kvs->list.level; i >= 1; i--) {
		while (x->forward[i]->key < start_key)
			x = x->forward[i];
	}

	while (x->forward[1]->key <= end_key) {
		// 여기있는것들이 결과가 되어야 함.
		x = x->forward[1];
		KVS_NODE_T *node = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
		node->forward = (KVS_NODE_T **)malloc(sizeof(KVS_NODE_T *) * 2);

		node->key = x->key;
		node->value = x->value;
		tmp->forward[1] = node;
		tmp = node;
		*num_entries += 1;
	}

	if (*num_entries != 0) {
		// unlock
		KVS_UNLOCK(&kvs, lock);

		return ret;
	}
	else {
		// unlock
		KVS_UNLOCK(&kvs, lock);

		free(ret->forward);
		free(ret);
		return NULL;
	}
#endif
}

/**
* @kvs	: key, value를 저장하는 KVS_T 구조체
* @key	: kvs에서 삭제할 key
* 삭제 성공시 1, 실패시 0, 에러 발생시 -1을 리턴한다.
*/
int kv_delete(KVS_T *kvs, int key) {
#if USING_LIST
	if (kvs == NULL)
		return -1;
	LIST_HEAD_T *p_node, *tmp;
	KVS_NODE_T *k_node;

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_wrlock(&kvs->lock);
#endif

	list_for_each_safe(p_node, tmp, &kvs->list)
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if (k_node->key == key) {
			list_remove(&k_node->list);
			free(k_node);
			kvs->size -= 1;

			// unlock
			KVS_UNLOCK(&kvs, lock);

			return 1;
		}
	}

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return 0;
#elif USING_HASH_TABLE
	if (kvs == NULL)
		return -1;
	LIST_HEAD_T *p_node, *tmp;
	KVS_NODE_T *k_node;
	int hash = get_hash(key);

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_wrlock(&kvs->lock);
#endif

	list_for_each_safe(p_node, tmp, hashtable_get(&kvs->ht, hash))
	{
		k_node = list_entry(p_node, KVS_NODE_T, list);
		if (k_node->key == key) {
			list_remove(&k_node->list);
			free(k_node);
			kvs->size -= 1;

			// unlock
			KVS_UNLOCK(&kvs, lock);

			return 1;
		}
	}

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return 0;
	///////////////////////////////
#elif USING_SKIP_LIST
	if (kvs == NULL)
		return -1;

	// lock
#if LOCK_MODE_MUTEX
	pthread_mutex_lock(&kvs->lock);
#elif LOCK_MODE_RW
	pthread_rwlock_wrlock(&kvs->lock);
#endif

	int ret = skiplist_delete(&kvs->list, key);
	if (ret == 0)
		kvs->size -= 1;

	// unlock
	KVS_UNLOCK(&kvs, lock);

	return ret;
#endif
}
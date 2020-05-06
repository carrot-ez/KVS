#include "kvs.h"
#include<stdlib.h>

/**
* KVS_T ��ü�� �ʱ�ȭ�ϰ� ��ȯ�Ѵ�.
*/
KVS_T *kv_create_db() {
	KVS_T *kvs = (KVS_T *)malloc(sizeof(KVS_T));
	if (kvs == NULL) {
		printf("create error\n");
		return NULL;
	}

	// KVS������ �Բ� ������ �ʱ�ȭ
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

	// KVS�� size�� �˱����� �ϳ��� ���� �߰�
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

	// kvs�� ������ ����
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

	// kvs�� ������ ����
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

	// kvs�� ������ ����
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
* @kv_pair	: kvs�� ������ ���
* ������ 1, ������ 0, ������ -1�� ��ȯ�Ѵ�.
*/
int kv_put(KVS_T *kvs, KVS_NODE_T *kv_pair) {
#if USING_LIST
	if (kvs == NULL) {
		printf("kvs is null\n");
		return -1;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node = NULL;

	// ���� �� lock
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

			// ��ȯ �� lock ����
			KVS_UNLOCK(&kvs, lock);

			return 0;
		}
	}

	k_node = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	// �޸� �Ҵ� ���н�
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

	// hashtable�� ����� ��� hash ���ϱ�
	int hash = get_hash(kv_pair->key);

	// ���� �� lock
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

			// ��ȯ �� lock ����
			KVS_UNLOCK(&kvs, lock);

			return 0;
		}
	}

	k_node = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	// �޸� �Ҵ� ���н�
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

	// ���� �� lock
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
* @key	: kvs���� �˻��� ������ Ű
* �˻� ������ key�� �ش��ϴ� ��� KVS_NODE_T�� ��ȯ
* ���н� NULL�� ��ȯ
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
			// memcpy�� node�� list���� �����ϹǷ� ������ �� ���ɼ��� �ִٰ� �Ǵ��ߴ�.
			// ���� key, value���� ������ ��, �ڽŸ��� ����Ű�� node�� �ٲ� �� ��ȯ�Ͽ���.
			ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
			if (ret == NULL) { // �޸� �Ҵ� ����
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

	// �ؽ����̺� ���� hash�� ���ϱ�
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
			// memcpy�� node�� list���� �����ϹǷ� ������ �� ���ɼ��� �ִٰ� �Ǵ��ߴ�.
			// ���� key, value���� ������ ��, �ڽŸ��� ����Ű�� node�� �ٲ� �� ��ȯ�Ͽ���.
			ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
			if (ret == NULL) { // �޸� �Ҵ� ����
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
* @start_key	: �˻��� �������� ��Ÿ���� Ű
* @end_key		: �˻��� ���������� ��Ÿ���� Ű
* @num_entries	: �˻��� ������ �����ϱ� ���� ����
* �˻� ������ ���ǿ� �ش��ϴ� ������ ���� ����Ʈ KVS_NODE_T�� ��ȯ,
* �˻� ���н� NULL�� ��ȯ
*/
KVS_NODE_T *kv_get_range(KVS_T *kvs, int start_key, int end_key, int *num_entries) {
#if USING_LIST // list�� ����Ҷ��� ���
	if (kvs == NULL) {
		printf("kvs is null\n");
		return NULL;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node, *tmp, *ret;

	ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	if (ret == NULL) { // �޸� �Ҵ� ���н�
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
			if (tmp == NULL) { // �޸� �Ҵ� ����
				printf("malloc error\n");

				// �Լ� ���� �� lock ����
				pthread_mutex_unlock(&kvs->lock);

				return NULL;
			}
			tmp->key = k_node->key;
			tmp->value = k_node->value;

			list_append(&ret->list, &tmp->list);
			*num_entries += 1;
		}
	}

	if (*num_entries == 0) { // ã�� ����� ���� ��
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

	if (*num_entries == 0) { // ã�� ����� ���� ��
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
		// �����ִ°͵��� ����� �Ǿ�� ��.
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
* @kvs	: key, value�� �����ϴ� KVS_T ����ü
* @key	: kvs���� ������ key
* ���� ������ 1, ���н� 0, ���� �߻��� -1�� �����Ѵ�.
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
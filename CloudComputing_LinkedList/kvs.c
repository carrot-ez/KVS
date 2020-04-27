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
#endif

	// KVS�� size�� �˱����� �ϳ��� ���� �߰�
	kvs->size = 0;

	return kvs;
}

/**
* @kvs	: KVS
* kvs�� ��� ��带 �޸� �����ϰ�, kvs�� �޸� �����Ѵ�.
*/
int kv_destroy_db(KVS_T *kvs) {
	if (kvs == NULL) {
		printf("destroy error\n");
		return -1;
	}

	// kvs�� ������ ����
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
* @kv_pair	: kvs�� ������ ���
* ������ 1, ������ 0, ������ -1�� ��ȯ�Ѵ�.
*/
int kv_put(KVS_T *kvs, KVS_NODE_T *kv_pair) {
	if (kvs == NULL) {
		printf("kvs is null\n");
		return -1;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node = NULL;
	// hashtable�� ����� ��� hash ���ϱ�
#if USING_HASH_TABLE
	int hash = get_hash(kv_pair->key);
#endif

	// ���� �� lock
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

			// ��ȯ �� lock ����
#if LOCK_MODE_MUTEX
			pthread_mutex_unlock(&kvs->lock);
#elif LOCK_MODE_RW
			pthread_rwlock_unlock(&kvs->lock);
#endif

			return 0;
		}
	}

	k_node = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
	// �޸� �Ҵ� ���н�
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
* @key	: kvs���� �˻��� ������ Ű
* �˻� ������ key�� �ش��ϴ� ��� KVS_NODE_T�� ��ȯ
* ���н� NULL�� ��ȯ
*/
KVS_NODE_T *kv_get(KVS_T *kvs, int key) {
	if (kvs == NULL) {
		printf("kvs is null\n");
		return NULL;
	}
	LIST_HEAD_T *p_node;
	KVS_NODE_T *k_node, *ret;

	// �ؽ����̺� ���� hash�� ���ϱ�
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
			// memcpy�� node�� list���� �����ϹǷ� ������ �� ���ɼ��� �ִٰ� �Ǵ��ߴ�.
			// ���� key, value���� ������ ��, �ڽŸ��� ����Ű�� node�� �ٲ� �� ��ȯ�Ͽ���.
			ret = (KVS_NODE_T *)malloc(sizeof(KVS_NODE_T));
			if (ret == NULL) { // �޸� �Ҵ� ����
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
* @start_key	: �˻��� �������� ��Ÿ���� Ű
* @end_key		: �˻��� ���������� ��Ÿ���� Ű
* @num_entries	: �˻��� ������ �����ϱ� ���� ����
* �˻� ������ ���ǿ� �ش��ϴ� ������ ���� ����Ʈ KVS_NODE_T�� ��ȯ,
* �˻� ���н� NULL�� ��ȯ
*/
#if USING_LIST // list�� ����Ҷ��� ���
KVS_NODE_T *kv_get_range(KVS_T *kvs, int start_key, int end_key, int *num_entries) {
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

	// ��忡 ���� �� lock
	pthread_mutex_lock(&kvs->lock);

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

		// �Լ� ���� �� lock ����
		pthread_mutex_unlock(&kvs->lock);

		return NULL;
	}

	// �Լ� ���� �� lock ����
	pthread_mutex_unlock(&kvs->lock);

	return ret;
}
#endif

/**
* @kvs	: key, value�� �����ϴ� KVS_T ����ü
* @key	: kvs���� ������ key
* ���� ������ 1, ���н� 0, ���� �߻��� -1�� �����Ѵ�.
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
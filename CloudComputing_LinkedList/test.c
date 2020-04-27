#define _CRT_SECURE_NO_WARNINGS
#define HAVE_STRUCT_TIMESPEC

#include<stdio.h>
#include<stdlib.h>
#include<pthread.h>
#include<time.h>
#include<Windows.h>
#include "kvs.h"

#define SEQUENTIAL_WRITE 0
#define SEQUENTIAL_READ 1
#define RANDOM_WRITE 2
#define RANDOM_READ 3


#define NUM_WRITE_THREAD 1
#define NUM_READ_THREAD 9
#define LIMIT_NUM 1000000

typedef void (*THREAD_FUNC)(void*);

struct SHARED_STATE_T {
	pthread_mutex_t mu;
	pthread_cond_t	cv;
	int total;
	int num_inited;
	int num_done;
	int start;
};

struct THREAD_STATE_T {
	pthread_t tid;
	int rand_seed;	 /*1: 1-1000000, 2: 1000001-2000000*/
	time_t start;
	time_t finish;
	int done;
	struct KVS_T* kvs;
};

struct THREAD_ARG_T {
	int id;
	int thread_num;
	struct SHARED_STATE_T* ss;
	struct THREAD_STATE_T* ts;
	THREAD_FUNC tf;
};

// sequential read
void *s_search(void *args) {
	struct THREAD_STATE_T *ts = args;
	struct KVS_NODE_T *p_node;
	for (int i = 0; i < LIMIT_NUM; i++) {
		p_node = kv_get(ts->kvs, i);
		if(p_node) {
			ts->done += 1;
			free(p_node);
		}
		if (i % (LIMIT_NUM / 2) == 0) {
			printf("[thread %d] sequential read\n", ts->tid);
		}
	}
	return NULL;
}

// sequential write
void *s_insert(void *args) {
	struct THREAD_STATE_T *ts = args;
	struct KVS_NODE_T node;
	for (int i = 0; i < LIMIT_NUM; i++) {
		node.key = i;
		node.value = i;
		int ret = kv_put(ts->kvs, &node);
		// no error occurred
		if (ret != -1) {
			ts->done += 1;
		}
		if (i % (LIMIT_NUM / 2) == 0) {
			printf("[thread %d] sequential write\n", ts->tid);
		}
	}
	return NULL;
}

// random read
void *r_search(void *args) {
	struct THREAD_STATE_T *ts = args;
	struct KVS_NODE_T *p_node;
	srand(ts->rand_seed);
	
	// 중복값인지 확인하기위한 배열, 크기를 최소로하기 위해 char배열로 동적할당하였다.
	char *check = (char *)malloc(sizeof(char) * LIMIT_NUM + 1);
	memset(check, 0, sizeof(char) * LIMIT_NUM + 1);

	for (int i = 0; i < LIMIT_NUM; i++) {
		int rand_num = get_random();

		// 중복값이 아닐때까지 랜덤넘버를 생성한다.
		while (check[rand_num] == 1) {
			rand_num = get_random();
		}
		check[rand_num] = 1;

		p_node = kv_get(ts->kvs, rand_num);
		if (p_node) {
			ts->done += 1;
			free(p_node);
		}
		if (i % (LIMIT_NUM/2) == 0) {
			printf("[thread %d] random read\n", ts->tid);
		}
	}
	return NULL;
}

// random write
void *r_insert(void *args) {
	struct THREAD_STATE_T *ts = args;
	struct KVS_NODE_T node;
	srand(ts->rand_seed);

	// 중복값인지 확인하기위한 배열, 크기를 최소로하기 위해 char배열로 동적할당하였다.
	char *check = (char *)malloc(sizeof(char) * LIMIT_NUM + 1);
	memset(check, 0, sizeof(char) * LIMIT_NUM + 1);

	for (int i = 0; i < LIMIT_NUM; i++) {
		int rand_ul = get_random();
		// 중복값이 아닐때까지 랜덤넘버를 생성한다.
		while (check[rand_ul] == 1) {
			rand_ul = get_random();
		}
		check[rand_ul] = 1;

		node.key = rand_ul;
		node.value = rand_ul;
		int ret = kv_put(ts->kvs, &node);
		// no error occurred
		if (ret != -1) {
			ts->done += 1;
		}
		if (i % (LIMIT_NUM / 2) == 0) {
			printf("[thread %d] random write\n", ts->tid);
		}
	}
	return NULL;
}

void *thread_main(void* arg) {
	struct THREAD_ARG_T* thread_arg = arg;
	struct SHARED_STATE_T* ss = thread_arg->ss;
	struct THREAD_STATE_T* ts = thread_arg->ts;

	// lock
	pthread_mutex_lock(&ss->mu);

	// increase shared initialize-num var
	ss->num_inited++;

	// if all threads are ready
	if (ss->num_inited >= ss->total) {
		pthread_cond_broadcast(&ss->cv);
	}

	// during shared start flag is false
	while (!ss->start) {
		pthread_cond_wait(&ss->cv, &ss->mu);
	}
	// unlock
	pthread_mutex_unlock(&ss->mu);

	// start function
	ts->start = time(NULL);
	(thread_arg->tf)((void *)ts);
	ts->finish = time(NULL);

	// lock
	pthread_mutex_lock(&ss->mu);

	// increase shared done-thread-num var
	ss->num_done++;
	// if all threads are finished
	if (ss->num_done >= ss->total) {
		pthread_cond_broadcast(&ss->cv);
	}

	// unlock
	pthread_mutex_unlock(&ss->mu);

	return NULL;
}

void *do_benchmark(void *arg) {
	struct THREAD_ARG_T *args = arg;
	int thread_num = args->thread_num;
	int i, ret;
	double elapsed = 0, num_ops = 0;

	// create all threads
	for (i = 0; i < thread_num; i++) {
		pthread_create(&args[i].ts->tid, NULL, thread_main, &args[i]);
	}

	// lock
	pthread_mutex_lock(&(args->ss->mu)); 

	// wait all threads to be initialized
	while (args->ss->num_inited < thread_num) {
		pthread_cond_wait(&(args->ss->cv), &(args->ss->mu));
	}

	// set start flag
	args->ss->start = 1;
	
	// wakeup all thread
	pthread_cond_broadcast(&(args->ss->cv));

	// wait all threads to be ended
	while (args->ss->num_done < thread_num) {
		pthread_cond_wait(&(args->ss->cv), &(args->ss->mu));
	}

	// unlock
	pthread_mutex_unlock(&(args->ss->mu));

	// join all threads
	for (i = 0; i < thread_num; i++) {
		ret = pthread_join(args[i].ts->tid, NULL);
	}

	// results
	for (i = 0; i < thread_num; i++) {
		elapsed = difftime(args->ts[i].finish, args->ts[i].start);
		num_ops += args->ts[i].done;
	}

	// print results
	switch (args->id) {	
	case SEQUENTIAL_WRITE: // sequential write
		printf("\n[sequential write] ops = %f\n", num_ops / elapsed);
		break;
	case SEQUENTIAL_READ: // sequential read
		printf("\n[sequential read] ops = %f\n", num_ops / elapsed);
		break;
	case RANDOM_WRITE: // random write
		printf("\n[random write] ops = %f\n", num_ops / elapsed);
		break;
	case RANDOM_READ: // random read
		printf("\n[random read] ops = %f\n", num_ops / elapsed);
		break;
	}

	// free
	free(args->ss);
	free(args->ts);
	free(args);
}

// THREAD_ARG_T를 초기화하는 함수
struct THREAD_ARG_T *make_thread_args(int id, int thread_num, struct KVS_T *kvs, THREAD_FUNC tf) {
	struct THREAD_ARG_T *args;
	struct SHARED_STATE_T *shared;
	struct THREAD_STATE_T* tss;

	/* initialize SHARED_STATE_T */
	shared = (struct SHARED_STATE_T *)malloc(sizeof(struct SHARED_STATE_T));
	pthread_mutex_init(&shared->mu, NULL);
	pthread_cond_init(&shared->cv, NULL);
	shared->total = thread_num;
	shared->num_done = 0;
	shared->num_inited = 0;
	shared->start = 0;

	/* initialize THREAD_ARG_T Array */
	args = (struct THREAD_ARG_T *)malloc(sizeof(struct THREAD_ARG_T) * thread_num);
	if (args == NULL) {
		printf("args malloc failed\n");
		return;
	}
	memset((void*)args, 0x00, sizeof(struct THREAD_ARG_T) * thread_num);
	args->id = id;
	args->thread_num = thread_num;

	/* initialize THREAD_STATE_T */
	tss = (struct THREAD_STATE_T*) malloc(sizeof(struct THREAD_STATE_T) * thread_num);
	if (tss == NULL) {
		printf("tss malloc failed\n");
		free(args);
		return;
	}
	memset((void*)tss, 0x00, sizeof(struct THREAD_STATE_T) * thread_num);

	/* initialize THREAD_ARG_T elements */
	for (int i = 0; i < thread_num; i++) {
		args[i].ss = shared;
		args[i].ts = &tss[i];
		args[i].ts->kvs = kvs;
		args[i].ts->rand_seed = time(NULL) + i;
		args[i].tf = tf;
	}

	return args;
}

// custom random generator
int get_random() {
	unsigned long r = rand();
	r = r << 15;
	r = r ^ rand();
	r = (r % LIMIT_NUM) + 1;
	return r;
}


int main(void)
{
	printf("-----------------------------------------\n");
	printf("   클라우드 컴퓨팅 \n");
	printf("   2015920063 컴퓨터과학부 홍찬표\n");
	printf("-----------------------------------------\n\n");

	// create kvs
	KVS_T *my_kvs;
	my_kvs = kv_create_db();
	if (my_kvs == NULL) {
		printf("kvs create error\n");

		getchar();
		return -1;
	}

	// put node
	struct KVS_NODE_T node;
	for (int i = 0; i < LIMIT_NUM; i++) {
		node.key = i;
		node.value = i;
		kv_put(my_kvs, &node);
	}

	struct THREAD_ARG_T *args[4];
	args[0] = make_thread_args(SEQUENTIAL_WRITE, NUM_WRITE_THREAD, my_kvs, s_insert);
	args[1] = make_thread_args(SEQUENTIAL_READ, NUM_READ_THREAD, my_kvs, s_search);
	args[2] = make_thread_args(RANDOM_WRITE, NUM_WRITE_THREAD, my_kvs, r_insert);
	args[3] = make_thread_args(RANDOM_READ, NUM_READ_THREAD, my_kvs, r_search);
	pthread_t thread[4];
	for (int i = 0; i < 2; i++) {
		pthread_create(&thread[i], NULL, do_benchmark, args[i]);
	}

	for (int i = 0; i < 2; i++) {
		pthread_join(thread[i], NULL);
	}


	// destroy kvs 
	kv_destroy_db(my_kvs);

	printf("end successfully \n");

	getchar();

	return 0;
}

/* 5주차 과제 */

//// total run time
//float sr_time = 0;
//float sw_time = 0;
//float rr_time = 0;
//float rw_time = 0;
//
//// flag
//volatile static int flag = 1;
//
//pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
//pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
//
//// thread가 실행하는 함수들에 사용하는 매개변수 구조체 선언
//struct search_p {
//	int thread_num;
//	KVS_T *kvs;
//};
//
//struct insert_p {
//	int thread_num;
//	KVS_T *kvs;
//};
//
//// create sequential read 
//void *sequential_read(void *args) {
//	/* using pthread condition */
//	pthread_cond_wait(&cond, &mutex);
//
//	/* using flag */
//	//while (flag == 1);
//
//	int start = clock();
//
//	struct search_p *data = args;
//	for (int i = 1; i <= LIMIT_NUM; i++) {
//		KVS_NODE_T *k_node = kv_get(data->kvs, i);
//		if (k_node != NULL)
//			free(k_node);
//	}
//
//	int end = clock();
//
//	/* using flag */
//	//pthread_mutex_lock(&mutex);
//
//	sr_time += (float)(end - start) / CLOCKS_PER_SEC;
//	printf("[thread %d] : complete sequential read %.3f sec\n", data->thread_num, (float)(end - start) / CLOCKS_PER_SEC);
//
//	pthread_mutex_unlock(&mutex);
//	return NULL;
//}
//
//// create sequential write
//void *sequential_write(void *args) {
//	/* using pthread condition */
//	pthread_cond_wait(&cond, &mutex);
//
//	/* using flag */
//	//while (flag == 1);
//
//	int start = clock();
//
//	struct insert_p *data = args;
//	for (int i = 1; i <= LIMIT_NUM; i++) {
//		KVS_NODE_T node;
//		node.key = i;
//		node.value = i;
//
//		int res = kv_put(data->kvs, &node);
//	}
//
//	int end = clock();
//
//	/* using flag */
//	//pthread_mutex_lock(&mutex);
//
//	sw_time += (float)(end - start) / CLOCKS_PER_SEC;
//	printf("[thread %d] : complete sequential write %.3f sec\n", data->thread_num, (float)(end - start) / CLOCKS_PER_SEC);
//
//	pthread_mutex_unlock(&mutex);
//	return NULL;
//}
//
//// create random read
//void *random_read(void * args) {
//	/* using pthread condition */
//	pthread_cond_wait(&cond, &mutex);
//
//	/* using flag */
//	//while (flag == 1);
//
//	int start = clock();
//
//	struct search_p *data = args;
//
//	// thread마다 다른 seed를 주기 위해 thread id를 이용
//	srand(time(NULL) + data->thread_num);
//
//	// bool타입 배열을 대신하는 char배열, 0 == 방문한 적 없는 값, 1 == 방문한 적 있는 값
//	char *random = (char *)malloc(sizeof(char) * (LIMIT_NUM + 1));
//	memset(random, 0, sizeof(char)*(LIMIT_NUM + 1));
//
//	int count = 0;
//	while (count < LIMIT_NUM) {
//		// unsigned long 범위의 1~LIMIT_NUM 사이의 random integer
//		// rnad() 함수가 0x7fff까지밖에 표현하지 못해 random함수를 따로 생성하였다.
//		unsigned long r = get_random();
//
//		// 방문한 적 없는 값이면
//		if (random[r] == 0) {
//			random[r] = 1;
//			KVS_NODE_T *k_node = kv_get(data->kvs, r);
//			if (k_node != NULL) {
//				free(k_node);
//			}
//
//			count += 1;
//		}
//	}
//	free(random);
//
//	int end = clock();
//
//	/* using flag */
//	//pthread_mutex_lock(&mutex);
//
//	rr_time += (float)(end - start) / CLOCKS_PER_SEC;
//	printf("[thread %d] : complete random read %.3f sec\n", data->thread_num, (float)(end - start) / CLOCKS_PER_SEC);
//
//	pthread_mutex_unlock(&mutex);
//	return NULL;
//}
//
//// create random write
//void *random_write(void * args) {
//	/* using pthread condition */
//	pthread_cond_wait(&cond, &mutex);
//
//	/* using flag */
//	//while (flag == 1);
//
//	int start = clock();
//
//	struct insert_p *data = args;
//
//	// thread마다 다른 seed를 주기 위해 thread id를 이용
//	srand(time(NULL) + data->thread_num);
//
//	// bool타입 배열을 대신하는 char배열, 0 == 방문한 적 없는 값, 1 == 방문한 적 있는 값
//	char *random = (char *)malloc(sizeof(char) * (LIMIT_NUM + 1));
//	memset(random, 0, sizeof(char)*(LIMIT_NUM + 1));
//
//	int count = 0;
//	while (count < LIMIT_NUM) {
//		// unsigned long 범위의 1~LIMIT_NUM 사이의 random integer
//		// rnad() 함수가 0x7fff까지밖에 표현하지 못해 random함수를 따로 생성하였다.
//		unsigned long r = get_random();
//
//		// 방문한 적 없는 값이면
//		if (random[r] == 0) {
//			random[r] = 1;
//			KVS_NODE_T k_node;
//			k_node.key = r;
//			k_node.value = r;
//			int res = kv_put(data->kvs, &k_node);
//
//			count += 1;
//		}
//	}
//	free(random);
//
//	int end = clock();
//
//	/* using flag */
//	//pthread_mutex_lock(&mutex);
//
//	rw_time += (float)(end - start) / CLOCKS_PER_SEC;
//	printf("[thread %d] : complete random write %.3f sec\n", data->thread_num, (float)(end - start) / CLOCKS_PER_SEC);
//
//	pthread_mutex_unlock(&mutex);
//	return NULL;
//}

/* 이전 과제에 사용한 함수 */

//struct search_range_p {
//	int thread_num;
//	KVS_T *kvs;
//	int start_key;
//	int end_key;
//};
//struct delete_p {
//	int thread_num;
//	KVS_T *kvs;
//};

/**
* @data	: search_p 구조체
* KVS와 key를 입력받고, 성공시 key와 value를 출력합니다.
*/
// void *thread_search(void *data) {
// 	KVS_T *kvs = ((struct search_p *)data)->kvs;
// 	int key = ((struct search_p *)data)->key;

// 	KVS_NODE_T *k_node;
// 	k_node = kv_get(kvs, key);

// 	if (k_node == NULL) {
// 		printf("[search(%d)]:  SEARCH FAILED\n", key);
// 	}
// 	else {
// 		printf("[search(%d)]:  key = %d,	value = %d\n", key, k_node->key, k_node->value);
// 	}
// 	return NULL;
// }

/**
* data	: search_range_p 구조체
* KVS와 start_key와 end_key를 입력받아 range검색을 수행하고,
* 성공시 결과값으로 찾은 원소의 개수를 출력합니다.
*/
//void *thread_search_range(void *data) {
//	KVS_T *kvs = ((struct search_range_p *)data)->kvs;
//	int start_key = ((struct search_range_p *)data)->start_key;
//	int end_key = ((struct search_range_p *)data)->end_key;
//
//	KVS_NODE_T *k_node, *tmp;
//	int num_entries = 0;
//	k_node = kv_get_range(kvs, start_key, end_key, &num_entries);
//	if (num_entries == 0) {
//		printf("[search_range(%d-%d)]:  SEARCH FAILED\n", start_key, end_key);
//	}
//	else {
//		printf("[search_range(%d-%d)]:  num_entries = %d\n", start_key, end_key, num_entries);
//	}
//
//	return NULL;
//}

/**
* data	: insert_p 구조체
* KVS와 num을 입력받고, 0부터 num까지 반복적으로 KVS에 값을 삽입합니다.
* 매 반복에서 삽입에 성공했을 시 전역변수 successInsert를 1 증가시킵니다.
* 성공한 횟수를 출력합니다.
*/
//void *thread_insert(void *data) {
//	KVS_T *kvs = ((struct insert_p *)data)->kvs;
//
//	for (int i = 0; i < LIMIT_NUM; i++) {
//		KVS_NODE_T tmp;
//		tmp.key = i;
//		tmp.value = i + 1;
//		int ret = kv_put(kvs, &tmp);
//
//		// if insert success
//		if (ret == 1) {
//			successInsert += 1;
//		}
//	}
//
//	printf("[insert_thread]:  success = %d\n", successInsert);
//
//	return NULL;
//}

/**
* data	: delete_p 구조체
* KVS와 num을 입력받고, 0부터 num까지 반복하며 삭제연산을 수행합니다.
* 삭제에 성공할 시 successDelete를 1 증가시킵니다.
* 성공적으로 삭제한 횟수를 출력합니다.
*/
//void *thread_delete(void *data) {
//	KVS_T *kvs = ((struct delete_p *)data)->kvs;
//
//	for (int i = 0; i < LIMIT_NUM; i++) {
//		int ret = kv_delete(kvs, i);
//
//		// if delete success
//		if (ret) {
//			successDelete += 1;
//		}
//	}
//
//	printf("[delete_thread]:  success = %d\n", successDelete);
//
//	return NULL;
//}
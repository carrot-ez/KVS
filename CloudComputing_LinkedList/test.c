#define _CRT_SECURE_NO_WARNINGS
#define HAVE_STRUCT_TIMESPEC

#include "kvs.h"
#include "skiplist.h"

#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include<Windows.h>
#include<pthread.h>


#define SEQUENTIAL_WRITE 0
#define SEQUENTIAL_READ 1
#define RANDOM_WRITE 2
#define RANDOM_READ 3


#define NUM_WRITE_THREAD 1
#define NUM_READ_THREAD 9
// num of test node
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
	KVS_NODE_T node;
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
	KVS_NODE_T node;
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
	KVS_NODE_T node;
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
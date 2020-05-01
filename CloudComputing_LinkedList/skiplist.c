#pragma once
///////////////////////////////////////////////////////////////////////
// 출처 : https://www.sanfoundry.com/c-program-implement-skip-list/  //
///////////////////////////////////////////////////////////////////////

#include "skiplist.h"

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>

skiplist *skiplist_init(skiplist* list) {
    int i;
    snode *header = (snode *)malloc(sizeof(struct snode));
    list->header = header;
    header->key = INT_MAX;
    header->forward = (snode **)malloc(
        sizeof(snode *) * (SKIPLIST_MAX_LEVEL + 1));
    for (i = 0; i <= SKIPLIST_MAX_LEVEL; i++) {
        header->forward[i] = list->header;
    }

    list->level = 1;
    list->size = 0;

    return list;
}

static int rand_level() {
    int level = 1;
    // 50% 확률로 레벨 증가
    while (rand() < RAND_MAX / 2 && level < SKIPLIST_MAX_LEVEL)
        level++;
    return level;
}

int skiplist_insert(skiplist* list, snode *node) {
    snode* update[SKIPLIST_MAX_LEVEL + 1];
    snode* x = list->header;
    int i, level;

    for (i = list->level; i >= 1; i--) {
        // x의 다음 노드의 키가 삽입할 키보다 크기 전까지 반복
        while (x->forward[i]->key < node->key)
            x = x->forward[i];
        update[i] = x; // 각 레벨에서 insert key 바로 전 노드를 저장
    }
    x = x->forward[1];

    if (node->key == x->key) {
        x->value = node->value;
        return 0;
    }
    else {
        level = rand_level();
        // 새로 추가할 노드의 레벨이 기존 리스트의 레벨보다 크면
        if (level > list->level) { 
            for (i = list->level + 1; i <= level; i++) {
                update[i] = list->header; // 초과되는 레벨에 대해 list->header로 초기화
            }
            list->level = level;
        }

        // 삽입할 노드 초기화
        x = (snode*)malloc(sizeof(snode));
        x->key = node->key;
        x->value = node->value;
        x->forward = (snode**)malloc(sizeof(snode*) * (level + 1));

        // update를 사용하여 새로운 노드를 연결
        for (i = 1; i <= level; i++) {
            x->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = x;
        }
    }
    return 0;
}

snode* skiplist_search(skiplist* list, int key) {
    snode* x = list->header;
    int i;
    for (i = list->level; i >= 1; i--) {
        while (x->forward[i]->key < key)
            x = x->forward[i];
    }
    if (x->forward[1]->key == key) {
        return x->forward[1];
    }
    else {
        return NULL;
    }
    return NULL;
}

// snode memory free
static void skiplist_node_free(snode* x) {
    if (x) {
        free(x->forward);
        free(x);
    }
}

int skiplist_delete(skiplist* list, int key) {
    int i;
    snode* update[SKIPLIST_MAX_LEVEL + 1];
    snode* x = list->header;

    // set update array
    for (i = list->level; i >= 1; i--) {
        while (x->forward[i]->key < key)
            x = x->forward[i];
        update[i] = x;
    }
    x = x->forward[1];

    if (x->key == key) {
        for (i = 1; i <= list->level; i++) {
            if (update[i]->forward[i] != x)
                break;
            update[i]->forward[i] = x->forward[i]; // x의 전 노드와 x의 다음노드를 연결
        }
        skiplist_node_free(x);

        // adjust level
        while (list->level > 1 && list->header->forward[list->level]
            == list->header)
            list->level--;
        return 0;
    }

    return 1;
}

// clear all elements
int skiplist_clear(skiplist *list) {
	struct snode *x, *tmp;
	
	for (x = list->header->forward[1], tmp = x->forward[1];
		x != list->header; x = tmp, tmp = x->forward[1]) {
		skiplist_node_free(x);
	}

    for (int i = 0; i <= SKIPLIST_MAX_LEVEL; i++) {
        list->header->forward[i] = list->header;
    }

    list->level = 1;
    list->size = 0;

	return 0;
}

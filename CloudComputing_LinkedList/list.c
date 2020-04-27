#include<stdio.h>
#include "list.h"

/* 리스트를 처음 생성할 때 수행 */
void list_init(struct LIST_HEAD_T *list)
{
	list->next = list;
	list->prev = list;
}

/* 리스트에 원소가 없으면 1을, 1개라도 있으면 0을 반환 */
int list_empty(struct LIST_HEAD_T *list)
{
	return list->next == list;
}

/* link의 앞에 new link를 삽입 */
void list_insert(struct LIST_HEAD_T *link, struct LIST_HEAD_T *new_link)
{
	new_link->prev = link->prev;
	new_link->next = link;
	new_link->prev->next = new_link;
	new_link->next->prev = new_link;
}

void list_append(struct LIST_HEAD_T *list, struct LIST_HEAD_T *new_link)
{
	list_insert((struct LIST_HEAD_T *)list, new_link);
}

/* link의 뒤에 new link를 삽입 */
void list_prepend(struct LIST_HEAD_T *list, struct LIST_HEAD_T *new_link)
{
	list_insert(list->next, new_link);
}

/* link의 연결을 헤제한다. */
void list_remove(struct LIST_HEAD_T *link)
{
	link->prev->next = link->next;
	link->next->prev = link->prev;
	link->prev = link->next = NULL;
}
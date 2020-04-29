#pragma once
/**
* link를 포함하는 type(구조체)의 첫 주소를 얻는다.
* @link		: List 구조체
* @type		: link를 포함시켜 리스트를 이루는 구조체의 타입
* @member	: @type 구조체에서 정의하는 List구조체의 변수명
*/
#define list_entry(link, type, member)									\
    ((type *)((char *)(link)-(unsigned long)(&((type *)0)->member)))

/* list(List 구조체)와 연결된 다음 구조체의 첫 주소를 얻는다. */
#define list_head(list, type, member)									\
    list_entry((list)->next, type, member)

/* list(List 구조체)와 연결된 이전 구조체의 첫 주소를 얻는다. */
#define list_tail(list, type, member)									\
    list_entry((list)->prev, type, member)

/**
* elm(Node 구조체)와 연결된 다음 Node구조체의 첫 주소를 얻는다.
* @elm		: Node 구조체
* @member	: Node 구조체가 포함하는 List 구조체 변수명
*/
#define list_next(elm, member, type)										    \
    list_entry((elm)->member.next, type, member)

/**
* list의 다음부터 리스트의 모든 원소를 접근하는 for 루프
* pos는 list의 다음 링크가 가리키는 Node 구조체의 첫 주소를 가져온다(다음 링크가 가리키는 Node구조체와 같다.)
* pos가 포함하는 List구조체가 처음 시작과 같아진다면 한바퀴를 돈 것이기 때문에 그 전까지 반복문을 수행한다.
* pos는 매 루프마다 다음 링크로 이동한다.
* @pos		: Node 구조체
* @list		: List 구조체
* @member	: Node 구조체에서 정의한 List 구조체의 변수명
*/
#define list_for_each_entry(pos, list, member, type)							\
for (pos = list_head(list, type, member);								\
         &pos->member != (list);										\
         pos = list_next(pos, member, type))

#define list_for_each(pos, head) \
	for(pos = (head)->next; pos != (head); pos = pos->next)

#define list_for_each_safe(pos, n, head) \
	for(pos = (head)->next, n = pos->next; pos != (head); \
		pos = n, n = pos->next)

/* link를 포함하는 type 구조체의 할당된 메모리를 해제한다. */
#define list_free(link, type, member)		\
free(list_entry(link, type, member));


typedef struct LIST_HEAD_T {
    struct LIST_HEAD_T* next, * prev;
}LIST_HEAD_T;


/* 리스트를 처음 생성할 때 수행 */
void list_init(struct LIST_HEAD_T* list);

/* 리스트에 원소가 없으면 1을, 1개라도 있으면 0을 반환 */
int list_empty(struct LIST_HEAD_T* list);

/* link의 앞에 new link를 삽입 */
void list_insert(struct LIST_HEAD_T* link, struct LIST_HEAD_T* new_link);

void list_append(struct LIST_HEAD_T* list, struct LIST_HEAD_T* new_link);

/* link의 뒤에 new link를 삽입 */
void list_prepend(struct LIST_HEAD_T* list, struct LIST_HEAD_T* new_link);

/* link의 연결을 헤제한다. */
void list_remove(struct LIST_HEAD_T* link);
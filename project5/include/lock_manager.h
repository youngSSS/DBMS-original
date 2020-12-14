#ifndef __LOCK_MANAGER_H__
#define __LOCK_MANAGER_H__

#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <vector>
#include <unordered_map>

using namespace std;

typedef struct lock_t lock_t;
typedef struct lock_table_entry lock_table_entry;

struct lock_table_entry {
    int table_id;
    int64_t key;
    lock_t * head;
    lock_t * tail;
};

struct lock_t {
	int trx_id;
	int lock_mode;
	int is_waiting;
	int undo;
	lock_table_entry * sentinel;
	lock_t * prev;
	lock_t * next;
	lock_t * trx_prev;
	lock_t * trx_next;
	pthread_cond_t cond;
};


/* APIs for lock manager */
int init_lock_table();
int detect_deadlock(int trx_id, lock_t * lock_obj);
lock_t* lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj, int abort_flag);
void lock_get_latch();
void lock_release_latch();
void print_lock_table();

#endif /* __LOCK_MANAGER_H__ */

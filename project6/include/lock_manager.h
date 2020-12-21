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

	// lock list
	lock_table_entry * sentinel;
	lock_t * prev;
	lock_t * next;

	// trx list
	lock_t * trx_prev;
	lock_t * trx_next;

	// wait
	pthread_cond_t cond;
};


/* APIs for lock manager */
int init_lock_table();
int detect_deadlock(int trx_id, lock_t * lock_obj);
int lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode, lock_t ** ret_lock, pthread_mutex_t * page_latch);
void send_signal(lock_t * lock_obj);
int lock_release(lock_t* lock_obj, int abort_flag);
void lock_wait(lock_t * lock_obj);
void lock_lock_latch();
void release_lock_latch();
void print_lock_table();

#endif /* __LOCK_MANAGER_H__ */

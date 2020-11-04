#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <unordered_map>

using namespace std;

typedef struct lock_t lock_t;
typedef struct hash_table_entry hash_table_entry;


struct lock_t {
	hash_table_entry * sentinel;
	lock_t * prev;
	lock_t * next;
	pthread_cond_t cond;
};

struct hash_table_entry {
	int table_id;
	int64_t key;
	lock_t * head;
	lock_t * tail;
};


/* APIs for lock table */
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key);
int lock_release(lock_t* lock_obj);

#endif /* __LOCK_TABLE_H__ */

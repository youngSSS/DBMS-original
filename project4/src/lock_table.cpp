#include <lock_table.h>

unordered_map< int, unordered_map< int64_t, hash_table_entry > > lock_hash_table;
pthread_mutex_t lock_table_latch;


int init_lock_table() {
	int result;
	result = pthread_mutex_init(&lock_table_latch, NULL);
	if (result != 0) printf("pthread_mutex_init() error\n");
	return 0;
}


lock_t * lock_acquire(int table_id, int64_t key) {

	pthread_mutex_lock(&lock_table_latch);
	
	hash_table_entry table_entry;
	lock_t * lock_obj;

	lock_obj = (lock_t*)malloc(sizeof(lock_t));
	if (lock_obj == NULL) return NULL;

	if (lock_hash_table[table_id].find(key) == lock_hash_table[table_id].end()) {
		
		table_entry.table_id = table_id;
		table_entry.key = key;
		table_entry.head = lock_obj;
		table_entry.tail = lock_obj;

		lock_hash_table[table_id][key] = table_entry;

		lock_obj->sentinel = &lock_hash_table[table_id][key];
		lock_obj->prev = NULL;
		lock_obj->next = NULL;

	}

	else {

		lock_obj->sentinel = &lock_hash_table[table_id][key];
		lock_obj->prev = lock_hash_table[table_id][key].tail;
		lock_obj->next = NULL;
		pthread_cond_init(&lock_obj->cond, NULL);

		lock_hash_table[table_id][key].tail->next = lock_obj;

		lock_hash_table[table_id][key].tail = lock_obj;

		pthread_cond_wait(&lock_obj->cond, &lock_table_latch);

	}

	pthread_mutex_unlock(&lock_table_latch);

	return lock_obj;
}


int lock_release(lock_t * lock_obj) {

	pthread_mutex_lock(&lock_table_latch);

	if (lock_obj->next == NULL) 
		lock_hash_table[lock_obj->sentinel->table_id].erase(lock_obj->sentinel->key);

	else {
		lock_obj->sentinel->head = lock_obj->next;

		lock_obj->next->prev = NULL;

		pthread_cond_signal(&lock_obj->next->cond);
	}

	free(lock_obj);

	pthread_mutex_unlock(&lock_table_latch);

	return 0;
}
#include "lock_manager.h"
#include "transaction_manager.h"

unordered_map< int, unordered_map< int64_t, lock_table_entry > > Lock_Table;
pthread_mutex_t Lock_Latch;


int init_lock_table() {

	if (pthread_mutex_init(&Lock_Latch, NULL) != 0)
		printf("mutex initialize fault\n");
	
	return 0;
}


int detect_deadlock(int trx_id, lock_t * new_lock_obj) {

	/* Detect deadlock only below 3 cases
	 * It means only 3 cases come to this function
	 *
	 * Case 1 : prev is S mode working
	 *     Case 1-1 : lock_obj is X mode
	 *
	 * Case 2 : prev is X mode working
	 *     Case 2-1 : lock_obj is S/X mode
	 *
	 * Case 3 : prev is waiting
	 *     Case 3-1 : prev is S, lock_obj is S/X
	 *     Case 3-2 : prev is X, lock_obj is S/X
	 */

	unordered_map< int, int > wait_for_list, temp_wait_for_list;
	unordered_map< int, int >::iterator iter;
	lock_t * lock_obj, * temp_lock_obj;

	lock_obj = new_lock_obj->prev;

	/* Case 1 */
	if (lock_obj->is_waiting == 0 && lock_obj->lock_mode == 0) {

		if (new_lock_obj->lock_mode == 1) {
			temp_lock_obj = lock_obj;

			while (temp_lock_obj != NULL) {
				temp_wait_for_list = get_wait_for_list(wait_for_list, temp_lock_obj->trx_id);

				for (iter = temp_wait_for_list.begin(); iter != temp_wait_for_list.end(); iter++)
					wait_for_list[iter->first] = iter->second;

				temp_lock_obj = temp_lock_obj->prev;
			}

			if (wait_for_list.find(trx_id) == wait_for_list.end())
				return 0;
		}

	}

	/* Case 2 */
	else if (lock_obj->is_waiting == 0 && lock_obj->lock_mode == 1) {

		temp_wait_for_list = get_wait_for_list(wait_for_list, lock_obj->trx_id);

		for (iter = temp_wait_for_list.begin(); iter != temp_wait_for_list.end(); iter++)
			wait_for_list[iter->first] = iter->second;

		if (wait_for_list.find(trx_id) == wait_for_list.end())
			return 0;

	}

	/* Case 3 */
	else if (lock_obj->is_waiting == 1) {

		if (lock_obj->lock_mode == 0) {
			temp_lock_obj = lock_obj;

			while (temp_lock_obj->is_waiting == 1) {
				if (temp_lock_obj->lock_mode == 1) 
					break;
				temp_lock_obj = temp_lock_obj->prev;
			}

			/* Case : wait for lock is working lock */
			if (temp_lock_obj->is_waiting == 0) {

				while (temp_lock_obj != NULL) {
					temp_wait_for_list = get_wait_for_list(wait_for_list, temp_lock_obj->trx_id);

					for (iter = temp_wait_for_list.begin(); iter != temp_wait_for_list.end(); iter++)
						wait_for_list[iter->first] = iter->second;

					temp_lock_obj = temp_lock_obj->prev;
				}

				if (wait_for_list.find(trx_id) == wait_for_list.end())
					return 0;

			}

			/* Case : wait for lock is X mode waiting lock */
			else {

				temp_wait_for_list = get_wait_for_list(wait_for_list, temp_lock_obj->trx_id);

				for (iter = temp_wait_for_list.begin(); iter != temp_wait_for_list.end(); iter++)
					wait_for_list[iter->first] = iter->second;

				if (wait_for_list.find(trx_id) == wait_for_list.end())
					return 0;

			}

		}

		else if (lock_obj->lock_mode == 1) {
			temp_wait_for_list = get_wait_for_list(wait_for_list, lock_obj->trx_id);

			for (iter = temp_wait_for_list.begin(); iter != temp_wait_for_list.end(); iter++)
				wait_for_list[iter->first] = iter->second;

			if (wait_for_list.find(trx_id) == wait_for_list.end())
				return 0;
		}

	}

	else 
		printf("detect_deadlock fault\n");

	return 1;
}


lock_t * lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode) {

	pthread_mutex_lock(&Lock_Latch);
	
	lock_table_entry table_entry;
	lock_t * lock_obj, * working_lock_obj, * waiting_lock_obj;

	lock_obj = (lock_t*)malloc(sizeof(lock_t));
	if (lock_obj == NULL) return NULL;

	/* Case : first lock acquire to this record */
	if (Lock_Table[table_id].find(key) == Lock_Table[table_id].end()) {

		table_entry.table_id = table_id;
		table_entry.key = key;
		table_entry.head = lock_obj;
		table_entry.tail = lock_obj;

		Lock_Table[table_id][key] = table_entry;

		lock_obj->trx_id = trx_id;
		lock_obj->lock_mode = lock_mode;
		lock_obj->is_waiting = 0;
		lock_obj->sentinel = &Lock_Table[table_id][key];
		lock_obj->prev = NULL;
		lock_obj->next = NULL;
		lock_obj->trx_next = NULL;

		trx_linking(lock_obj, trx_id);

	}

	/* Case : record already has a lock list */
	else {
		working_lock_obj = Lock_Table[table_id][key].head;

		/* Case : working lock is X lock (= olny this lock is woring) */
		if (working_lock_obj->lock_mode == 1) {

			if (working_lock_obj->trx_id == trx_id) {
				free(lock_obj);
				pthread_mutex_unlock(&Lock_Latch);

				return working_lock_obj;
			}

		}

		/* Case : working lock is S lock */
		else {
			// Visit every working lock
			while (working_lock_obj->is_waiting == 0) {

				if (working_lock_obj->trx_id == trx_id) {

					/* Case : my lock is S mode, just return my trx's working lock */
					if (lock_mode == 0) {

						free(lock_obj);
						pthread_mutex_unlock(&Lock_Latch);

						return working_lock_obj;
					}

					/* Case : my lock is X mode
					 * if my trx's working lock is olny one which is working, upgrade it to X mode
					 * Else, wait until another trx's working lock is released
					 */
					else {
						/* Case : only 1 working lock exist, which is me */
						// Upgrade lock to X mode and return it's addr
						if (working_lock_obj == Lock_Table[table_id][key].head) {
							if (working_lock_obj->next == NULL) {

								working_lock_obj->lock_mode = 1;
								free(lock_obj);

                                pthread_mutex_unlock(&Lock_Latch);

								return working_lock_obj;
							}

							else {
								if (working_lock_obj->next->is_waiting == 1) {

									working_lock_obj->lock_mode = 1;
									free(lock_obj);

                                    pthread_mutex_unlock(&Lock_Latch);

									return working_lock_obj;
								}
							}
						}

						// If there is no waiting lock, No deadlock
						// Else, it's a deadlock -> trx1(S) - trx3(S) - trx2(X) - trx1(X)
						if (Lock_Table[table_id][key].tail->is_waiting == 0) {

							lock_obj->trx_id = trx_id;
							lock_obj->lock_mode = lock_mode;
							lock_obj->is_waiting = 1;
							lock_obj->sentinel = &Lock_Table[table_id][key];
							lock_obj->prev = Lock_Table[table_id][key].tail;
							lock_obj->next = NULL;
							lock_obj->trx_next = NULL;
							pthread_cond_init(&lock_obj->cond, NULL);

							Lock_Table[table_id][key].tail->next = lock_obj;
							Lock_Table[table_id][key].tail = lock_obj;

							trx_linking(lock_obj, trx_id);

							if (detect_deadlock(trx_id, lock_obj) == 1)
							    return NULL;

							pthread_cond_wait(&lock_obj->cond, &Lock_Latch);

							pthread_mutex_unlock(&Lock_Latch);

							return lock_obj;
						}

						/* Case : deadlock */
						else 
							return NULL;
						
					}
				}

				working_lock_obj = working_lock_obj->next;
				if (working_lock_obj == NULL) break;
			}

		}

		/* Case : my trx is not in working set */

		lock_obj->trx_id = trx_id;
		lock_obj->lock_mode = lock_mode;
		lock_obj->is_waiting = 0;
		lock_obj->sentinel = &Lock_Table[table_id][key];
		lock_obj->prev = Lock_Table[table_id][key].tail;
		lock_obj->next = NULL;
		lock_obj->trx_next = NULL;
		pthread_cond_init(&lock_obj->cond, NULL);

		Lock_Table[table_id][key].tail->next = lock_obj;
		Lock_Table[table_id][key].tail = lock_obj;

		trx_linking(lock_obj, trx_id);

		/* Case : prev is working lock */
		if (lock_obj->prev->is_waiting == 0) {

			/* Case : prev is shared lock */
			if (lock_obj->prev->lock_mode == 0) {

				/* Case : lock_obj is exclusive lock */
				if (lock_obj->lock_mode == 1) {
					// return NULL without releasing Lock_Latch
					if (detect_deadlock(trx_id, lock_obj) == 1)
						return NULL;

					else {
						lock_obj->is_waiting = 1;
						pthread_cond_wait(&lock_obj->cond, &Lock_Latch);
					}
				}
			}

			/* Case : prev is exclusive lock */
			else {
				// return NULL without releasing Lock_Latch
				if (detect_deadlock(trx_id, lock_obj) == 1)
					return NULL;
				
				else {
					lock_obj->is_waiting = 1;
					pthread_cond_wait(&lock_obj->cond, &Lock_Latch);
				}
			}

		}


		/* Case : prev is waiting lock */
		else {
			// return NULL without releasing Lock_Latch
			if (detect_deadlock(trx_id, lock_obj) == 1)
				return NULL;
			
			else {
				lock_obj->is_waiting = 1;
				pthread_cond_wait(&lock_obj->cond, &Lock_Latch);
			}
		}

	}

	pthread_mutex_unlock(&Lock_Latch);

	return lock_obj;
}


int lock_release(lock_t * lock_obj, int is_abort) {
    if (is_abort == 0)
	    pthread_mutex_lock(&Lock_Latch);

	lock_t * temp_lock_obj;
	int table_id;
	int64_t key;

	table_id = lock_obj->sentinel->table_id;
	key = lock_obj->sentinel->key;

	/* Send signal only when lock_obj is head and lock_obj's next lock is waiting */

	/* Case : lock_obj is head of lock list */
	if (Lock_Table[table_id][key].head == lock_obj) {

		/* Case : lock_obj is only lock of lock list */
		if (lock_obj->next == NULL)
			Lock_Table[table_id].erase(key);

		/* Case : lock list has another lock */
		else {
			lock_obj->sentinel->head = lock_obj->next;
			lock_obj->next->prev = NULL;

			/* Case : if next lock is waiting, send signal */
			if (lock_obj->next->is_waiting == 1) {

				if (lock_obj->next->lock_mode == 1) {
					lock_obj->next->is_waiting = 0;
					pthread_cond_signal(&lock_obj->next->cond);
				}

				else {
					temp_lock_obj = lock_obj->next;

					while (temp_lock_obj->lock_mode == 0) {
						temp_lock_obj->is_waiting = 0;
						pthread_cond_signal(&temp_lock_obj->cond);
						temp_lock_obj = temp_lock_obj->next;
						if (temp_lock_obj == NULL) break;
					}
				}
			}
		}

	}

	/* Case : lock_obj is not head of lock list */
	else {

		/* Case : if lock_obj is tail of lock list, change tail and lock connection */
		if (Lock_Table[table_id][key].tail == lock_obj) {
			lock_obj->prev->next = NULL;
			Lock_Table[table_id][key].tail = lock_obj->prev;
		}

		/* Case : if lock_obj is not tail, change only the connection */
		else {
			lock_obj->prev->next = lock_obj->next;
			lock_obj->next->prev = lock_obj->prev;
		}

	}

	/* Case : send signal to me ( S(trx1, working) -> X(trx1, waiting) ) */
	if (Lock_Table[table_id].find(key) != Lock_Table[table_id].end()) {
		// if stmt means that there is only 1 working lock
		if (Lock_Table[table_id][key].head->next != NULL && Lock_Table[table_id][key].head->next->is_waiting == 1) {

			if (Lock_Table[table_id][key].head->trx_id == Lock_Table[table_id][key].head->next->trx_id) {
				temp_lock_obj = Lock_Table[table_id][key].head;

				Lock_Table[table_id][key].head = Lock_Table[table_id][key].head->next;
				Lock_Table[table_id][key].head->next->prev = NULL;
				Lock_Table[table_id][key].head->next->is_waiting = 0;

				free(temp_lock_obj);

				pthread_cond_signal(&Lock_Table[table_id][key].head->next->cond);
			}
		}
	}
	
	free(lock_obj);

	if (is_abort == 0)
	    pthread_mutex_unlock(&Lock_Latch);

	return 0;
}

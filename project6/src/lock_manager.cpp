#include "lock_manager.h"
#include "transaction_manager.h"

unordered_map< int, unordered_map< int64_t, lock_table_entry * > > Lock_Table;
pthread_mutex_t Lock_Latch;

// return 0 : Success
// return 1 : Lock_Latch initialize fault
int init_lock_table() {
	if (pthread_mutex_init(&Lock_Latch, NULL) != 0) return 1;
	return 0;
}


// return 0 : No deadlock
// return 1 : Deadlock
int detect_deadlock(int trx_id, lock_t * lock_obj) {
    unordered_map< int, int > is_visit;
    unordered_map< int, int >::iterator iter;

    lock_trx_table_latch();
    is_visit = get_wait_for_list(lock_obj, is_visit);
    release_trx_table_latch();

    for (iter = is_visit.begin(); iter != is_visit.end(); iter++) {
        if (iter->first == trx_id) {

//            printf("Table id : %d, key : %d, trx_id(%d)\n",
//            lock_obj->sentinel->table_id, lock_obj->sentinel->key, lock_obj->trx_id);
//
//            print_lock_table();
//            printf("Wait For List : ");
//
//            if (is_visit.size() == 0)  {
//                printf("None");
//            }
//            for (iter = is_visit.begin(); iter != is_visit.end(); iter++) {
//                printf("%d, ", iter->first);
//            }
//            printf("\n");

            return 1;
        }
    }

    return 0;
}


int lock_list_existence_check(int table_id, int64_t key) {
    if (Lock_Table[table_id].find(key) == Lock_Table[table_id].end())
        return 0;
    else {
        if (Lock_Table[table_id][key] == NULL)
            return 0;
        else
            return 1;
    }
}


int lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode, lock_t ** ret_lock, pthread_mutex_t * page_latch) {

    pthread_mutex_lock(&Lock_Latch);

	lock_table_entry * table_entry;
	lock_t * lock_obj, * working_lock_obj, * target_lock_obj;
	int trx_exist_flag = 0;


	/* Case 1 : Nothing in lock list */
	if (lock_list_existence_check(table_id, key) == 0) {

	    // Make lock table entry
	    table_entry = (lock_table_entry*)malloc(sizeof(lock_table_entry));
        if (table_entry == NULL) {
            printf("Make table entry fault\n");
            pthread_mutex_unlock(&Lock_Latch);
            return 3;
        }

	    // Make lock object
        lock_obj = (lock_t*)malloc(sizeof(lock_t));
        if (lock_obj == NULL) {
            printf("Make lock object fault\n");
            pthread_mutex_unlock(&Lock_Latch);
            return 3;
        }

        // Push entry to lock table
        Lock_Table[table_id][key] = table_entry;

        // Initialize table entry
        table_entry->table_id = table_id;
        table_entry->key = key;
        table_entry->head = lock_obj;
        table_entry->tail = lock_obj;

        // Initialize lock object
        lock_obj->trx_id = trx_id;
        lock_obj->lock_mode = lock_mode;
        lock_obj->is_waiting = 0;
        lock_obj->sentinel = table_entry;
        lock_obj->prev = NULL;
        lock_obj->next = NULL;
        lock_obj->trx_prev = NULL;
        lock_obj->trx_next = NULL;
        pthread_cond_init(&lock_obj->cond, NULL);

        // Append lock_obj to Trx_Table
        trx_linking(lock_obj);

        *ret_lock = lock_obj;

        pthread_mutex_unlock(&Lock_Latch);

        return 0;

	}

	/* Case 2 : Something in lock list */
	else {

	    // Check whether my TRX exists in lock list
	    working_lock_obj = Lock_Table[table_id][key]->head;
	    while (working_lock_obj->is_waiting == 0) {
            if (working_lock_obj->trx_id == trx_id) {
                trx_exist_flag = 1;
                break;
            }
            working_lock_obj = working_lock_obj->next;
            if (working_lock_obj == NULL) break;
	    }

	    /* Case 2-1 : Lock list has its own TRX */
        if (trx_exist_flag == 1) {

            /* Case 2-1-1 : working_lock_obj is working lock */
            if (working_lock_obj->is_waiting == 0) {

                /* Case 2-1-1-1 : working_lock_obj is S mode working lock */
                if (working_lock_obj->lock_mode == 0) {

                    /* Case 2-1-1-1-1 : Trying to get S lock */
                    if (lock_mode == 0) {
                        *ret_lock = working_lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);
                        return 0;
                    }

                    /* Case 2-1-1-1-2 : Trying to get X lock */
                    else {

                        /* Case 2-1-1-1-2-1 : working_lock_obj is working alone */
                        if (Lock_Table[table_id][key]->head == working_lock_obj &&
                                (working_lock_obj->next == NULL || working_lock_obj->next->is_waiting == 1)) {
                            working_lock_obj->lock_mode = 1;
                            *ret_lock = working_lock_obj;
                            pthread_mutex_unlock(&Lock_Latch);
                            return 0;
                        }

                        /* Case 2-1-1-1-2-2 : working_lock_obj is working together */
                        else {

                            /* Case 2-1-1-1-2-2-1 : waiting lock exist in lock list */
                            if (Lock_Table[table_id][key]->tail->is_waiting == 1) {
                                pthread_mutex_unlock(&Lock_Latch);
                                return 2;
                            }

                            /* Case 2-1-1-1-2-2-1 : waiting lock does not exist in lock list */
                            else {

                                // Make lock object
                                lock_obj = (lock_t*)malloc(sizeof(lock_t));
                                if (lock_obj == NULL) {
                                    printf("Make lock object fault\n");
                                    pthread_mutex_unlock(&Lock_Latch);
                                    return 3;
                                }

                                // Initialize lock object
                                lock_obj->trx_id = trx_id;
                                lock_obj->lock_mode = lock_mode;
                                lock_obj->is_waiting = 1;
                                lock_obj->sentinel = Lock_Table[table_id][key];
                                lock_obj->prev = NULL;
                                lock_obj->next = NULL;
                                lock_obj->trx_prev = NULL;
                                lock_obj->trx_next = NULL;
                                pthread_cond_init(&lock_obj->cond, NULL);

                                // Link lock_obj and lock list
                                lock_obj->prev = Lock_Table[table_id][key]->tail;
                                Lock_Table[table_id][key]->tail->next = lock_obj;
                                Lock_Table[table_id][key]->tail = lock_obj;

                                // append lock_obj to Trx_Table
                                trx_linking(lock_obj);

                                // Deadlock detection
                                if (detect_deadlock(trx_id, lock_obj) == 1) {
                                    pthread_mutex_unlock(&Lock_Latch);
                                    return 2;
                                }

                                // Lock transaction latch
                                lock_trx_latch(lock_obj->trx_id);

                                *ret_lock = lock_obj;

                                pthread_mutex_unlock(&Lock_Latch);

                                return 1;
                            }

                        }

                    }

                }

                /* Case 2-1-1-2 : working_lock_obj is X mode working lock */
                else {

                    /* Case 2-1-1-2-1 : Trying to get S lock */
                    if (lock_mode == 0) {
                        *ret_lock = working_lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);
                        return 0;
                    }

                    /* Case 2-1-1-2-2 : Trying to get X lock */
                    else {
                        *ret_lock = working_lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);
                        return 0;
                    }

                }

            }

            /* Case 2-1-2 : working_lock_obj is waiting lock */
            else {
                // This case should never happen
                printf(
                        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 1 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                );
            }

        }

        /* Case 2-2 : Lock list does not have its own TRX */
        else {

            // target lock object is the lock which will be located at right in front of lock_obj
            target_lock_obj = Lock_Table[table_id][key]->tail;

            // Make lock object
            lock_obj = (lock_t*)malloc(sizeof(lock_t));
            if (lock_obj == NULL) {
                pthread_mutex_unlock(&Lock_Latch);
                return 3;
            }

            // Initialize lock object
            lock_obj->trx_id = trx_id;
            lock_obj->lock_mode = lock_mode;
            lock_obj->is_waiting = 1;
            lock_obj->sentinel = Lock_Table[table_id][key];
            lock_obj->prev = NULL;
            lock_obj->next = NULL;
            lock_obj->trx_prev = NULL;
            lock_obj->trx_next = NULL;
            pthread_cond_init(&lock_obj->cond, NULL);

            // Link lock_obj and lock list
            lock_obj->prev = Lock_Table[table_id][key]->tail;
            Lock_Table[table_id][key]->tail->next = lock_obj;
            Lock_Table[table_id][key]->tail = lock_obj;

            // append lock_obj to Trx_Table
            trx_linking(lock_obj);

            /* Case 2-2-1 : target is working lock */
            if (target_lock_obj->is_waiting == 0) {

                /* Case 2-2-1-1 : target is S mode working lock */
                if (target_lock_obj->lock_mode == 0) {

                    /* Case 2-2-1-1-1 : Trying to get S lock */
                    if (lock_obj->lock_mode == 0) {
                        lock_obj->is_waiting = 0;
                        *ret_lock = lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);
                        return 0;
                    }

                    /* Case 2-2-1-1-2 : Trying to get X lock */
                    else {
                        // Deadlock detection
                        if (detect_deadlock(trx_id, lock_obj) == 1) {
                            pthread_mutex_unlock(&Lock_Latch);
                            return 2;
                        }

                        // Lock transaction latch
                        lock_trx_latch(lock_obj->trx_id);

                        *ret_lock = lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);

                        return 1;
                    }

                }

                /* Case 2-2-1-2 : target is X mode working lock */
                else {

                    /* Case 2-2-1-2-1 : Trying to get S lock */
                    if (lock_obj->lock_mode == 0) {
                        // Deadlock detection
                        if (detect_deadlock(trx_id, lock_obj) == 1) {
                            pthread_mutex_unlock(&Lock_Latch);
                            return 2;
                        }

                        // Lock transaction latch
                        lock_trx_latch(lock_obj->trx_id);

                        *ret_lock = lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);

                        return 1;
                    }

                    /* Case 2-2-1-2-2 : Trying to get X lock */
                    else {
                        // Deadlock detection
                        if (detect_deadlock(trx_id, lock_obj) == 1) {
                            pthread_mutex_unlock(&Lock_Latch);
                            return 2;
                        }

                        // Lock transaction latch
                        lock_trx_latch(lock_obj->trx_id);

                        *ret_lock = lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);

                        return 1;
                    }

                }

            }

            /* Case 2-2-2 : target is waiting lock */
            else {

                /* Case 2-2-2-1 : target is S mode waiting lock */
                if (target_lock_obj->lock_mode == 0) {

                    /* Case 2-2-2-1-1 : Trying to get S lock */
                    if (lock_obj->lock_mode == 0) {
                        // Deadlock detection
                        if (detect_deadlock(trx_id, lock_obj) == 1) {
                            pthread_mutex_unlock(&Lock_Latch);
                            return 2;
                        }

                        // Lock transaction latch
                        lock_trx_latch(lock_obj->trx_id);

                        *ret_lock = lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);

                        return 1;
                    }

                    /* Case 2-2-2-1-2 : Trying to get X lock */
                    else {
                        // Deadlock detection
                        if (detect_deadlock(trx_id, lock_obj) == 1) {
                            pthread_mutex_unlock(&Lock_Latch);
                            return 2;
                        }

                        // Lock transaction latch
                        lock_trx_latch(lock_obj->trx_id);

                        *ret_lock = lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);

                        return 1;
                    }

                }

                /* Case 2-2-2-2 : target is X mode waiting lock */
                else {

                    /* Case 2-2-2-2-1 : Trying to get S lock */
                    if (lock_obj->lock_mode == 0) {
                        // Deadlock detection
                        if (detect_deadlock(trx_id, lock_obj) == 1) {
                            pthread_mutex_unlock(&Lock_Latch);
                            return 2;
                        }

                        // Lock transaction latch
                        lock_trx_latch(lock_obj->trx_id);

                        *ret_lock = lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);

                        return 1;
                    }

                    /* Case 2-2-2-2-2 : Trying to get X lock */
                    else {
                        // Deadlock detection
                        if (detect_deadlock(trx_id, lock_obj) == 1) {
                            pthread_mutex_unlock(&Lock_Latch);
                            return 2;
                        }

                        // Lock transaction latch
                        lock_trx_latch(lock_obj->trx_id);

                        *ret_lock = lock_obj;
                        pthread_mutex_unlock(&Lock_Latch);

                        return 1;
                    }

                }

            }

        }

	}

    // This case should never happen
    printf(
            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 2 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
    );

	return 4;
}


void send_signal(lock_t * lock_obj) {

    // lock_obj is S lock
    if (lock_obj->lock_mode == 0) {
        while (lock_obj->lock_mode == 0) {

            pthread_mutex_lock(get_trx_latch(lock_obj->trx_id));
            lock_obj->is_waiting = 0;
            pthread_cond_signal(&lock_obj->cond);
            pthread_mutex_unlock(get_trx_latch(lock_obj->trx_id));

            lock_obj = lock_obj->next;
            if (lock_obj == NULL) break;
        }
    }

    // lock_obj is X lock
    else {
        pthread_mutex_lock(get_trx_latch(lock_obj->trx_id));
        lock_obj->is_waiting = 0;
        pthread_cond_signal(&lock_obj->cond);
        pthread_mutex_unlock(get_trx_latch(lock_obj->trx_id));
    }

}


int lock_release(lock_t * lock_obj, int abort_flag) {

	lock_t * temp_lock_obj;
	int table_id;
	int64_t key;

    unordered_map< int, unordered_map< int64_t, lock_table_entry * > > temp;

	table_id = lock_obj->sentinel->table_id;
	key = lock_obj->sentinel->key;

	/* Case : Release by trx_commit */
	if (abort_flag == 0) {

	    /* Case 1 : lock_obj is working */
	    if (lock_obj->is_waiting == 0) {

	        temp = Lock_Table;

	        /* Case 1-1 : lock_obj is head and tail */
	        if (Lock_Table[table_id][key]->head == lock_obj && Lock_Table[table_id][key]->tail == lock_obj) {
                // Erase lock table entry
                free(Lock_Table[table_id][key]);
                Lock_Table[table_id].erase(key);
	        }

	        /* Case 1-2 : lock_obj is only head */
	        else if (Lock_Table[table_id][key]->head == lock_obj && Lock_Table[table_id][key]->tail != lock_obj) {

	            /* Case 1-2-1 : lock_obj is S lock */
                if (lock_obj->lock_mode == 0) {

                    /* Case 1-2-1-1 : next lock is working lock */
                    if (lock_obj->next->is_waiting == 0) {
                        Lock_Table[table_id][key]->head = lock_obj->next;
                        lock_obj->next->prev = NULL;
                    }

                    /* Case 1-2-1-2 : next lock is waiting lock */
                    else {
                        Lock_Table[table_id][key]->head = lock_obj->next;
                        lock_obj->next->prev = NULL;
                        send_signal(Lock_Table[table_id][key]->head);
                    }

                }

                /* Case 1-2-2 : lock_obj is X lock */
                else {

                    /* Case 1-2-2-1 : next lock is working lock */
                    if (lock_obj->next->is_waiting == 0) {
                        // This case should never happen
                        printf(
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 6 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                        );
                    }

                    /* Case 1-2-2-2 : next lock is waiting lock */
                    else {
                        Lock_Table[table_id][key]->head = lock_obj->next;
                        lock_obj->next->prev = NULL;
                        send_signal(Lock_Table[table_id][key]->head);
                    }

                }

	        }

	        /* Case 1-3 : lock_obj is only tail */
	        else if (Lock_Table[table_id][key]->head != lock_obj && Lock_Table[table_id][key]->tail == lock_obj) {

                /* Case 1-3-1 : lock_obj is S lock */
                if (lock_obj->lock_mode == 0) {
                    Lock_Table[table_id][key]->tail = lock_obj->prev;
                    lock_obj->prev->next = NULL;
                }

                /* Case 1-3-2 : lock_obj is X lock */
                else {
                    // This case should never happen
                    printf(
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 5 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    );
                }

	        }

	        /* Case 1-4 : lock_obj is not both of them */
	        else {

	            /* Case 1-4-1 : lock_obj is S lock */
                if (lock_obj->lock_mode == 0) {
                    lock_obj->prev->next = lock_obj->next;
                    lock_obj->next->prev = lock_obj->prev;
                }

                /* Case 1-4-2 : lock_obj is X lock */
                else {
                    print_lock_table();
                    // This case should never happen
                    printf(
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 4 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    );
                }

	        }

	    }

	    /* Case 2 : lock_obj is waiting */
	    else {
            // This case should never happen
            printf(
                    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 3 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
            );
	    }

        /* Case : send a signal to me ( S(trx1, working) -> X(trx1, waiting) ) */
        if (Lock_Table[table_id].find(key) != Lock_Table[table_id].end()) {
            // if stmt means that there is only 1 working lock
            if (Lock_Table[table_id][key]->head->next != NULL &&
                Lock_Table[table_id][key]->head->next->is_waiting == 1 &&
                Lock_Table[table_id][key]->head->trx_id == Lock_Table[table_id][key]->head->next->trx_id) {

                temp_lock_obj = Lock_Table[table_id][key]->head;

                Lock_Table[table_id][key]->head = Lock_Table[table_id][key]->head->next;
                Lock_Table[table_id][key]->head->prev = NULL;

                trx_cut_linking(temp_lock_obj);
                free(temp_lock_obj);

                send_signal(Lock_Table[table_id][key]->head);
            }
        }

	}


	/* Case : Release by trx_abort */
	else {

        /* Case 1 : lock_obj is working */
        if (lock_obj->is_waiting == 0) {

            /* Case 1-1 : lock_obj is head and tail */
            if (Lock_Table[table_id][key]->head == lock_obj && Lock_Table[table_id][key]->tail == lock_obj) {
                // Erase lock table entry
                free(Lock_Table[table_id][key]);
                Lock_Table[table_id].erase(key);
            }

            /* Case 1-2 : lock_obj is only head */
            else if (Lock_Table[table_id][key]->head == lock_obj && Lock_Table[table_id][key]->tail != lock_obj) {

                /* Case 1-2-1 : lock_obj is S lock */
                if (lock_obj->lock_mode == 0) {

                    /* Case 1-2-1-1 : next lock is working lock */
                    if (lock_obj->next->is_waiting == 0) {
                        Lock_Table[table_id][key]->head = lock_obj->next;
                        lock_obj->next->prev = NULL;
                    }

                    /* Case 1-2-1-2 : next lock is waiting lock */
                    else {
                        Lock_Table[table_id][key]->head = lock_obj->next;
                        lock_obj->next->prev = NULL;
                        send_signal(Lock_Table[table_id][key]->head);
                    }

                }

                /* Case 1-2-2 : lock_obj is X lock */
                else {

                    /* Case 1-2-2-1 : next lock is working lock */
                    if (lock_obj->next->is_waiting == 0) {
                        // This case should never happen
                        printf(
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 9 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                        );
                    }

                    /* Case 1-2-2-2 : next lock is waiting lock */
                    else {
                        Lock_Table[table_id][key]->head = lock_obj->next;
                        lock_obj->next->prev = NULL;
                        send_signal(Lock_Table[table_id][key]->head);
                    }

                }

            }

            /* Case 1-3 : lock_obj is only tail */
            else if (Lock_Table[table_id][key]->head != lock_obj && Lock_Table[table_id][key]->tail == lock_obj) {

                /* Case 1-3-1 : lock_obj is S lock */
                if (lock_obj->lock_mode == 0) {
                    Lock_Table[table_id][key]->tail = lock_obj->prev;
                    lock_obj->prev->next = NULL;
                }

                /* Case 1-3-2 : lock_obj is X lock */
                else {
                    // This case should never happen
                    printf(
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 8 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    );
                }

            }

            /* Case 1-4 : lock_obj is not both of them */
            else {

                /* Case 1-4-1 : lock_obj is S lock */
                if (lock_obj->lock_mode == 0) {
                    lock_obj->prev->next = lock_obj->next;
                    lock_obj->next->prev = lock_obj->prev;
                }

                /* Case 1-4-2 : lock_obj is X lock */
                else {
                    // This case should never happen
                    printf(
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 7 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    );
                }

            }

            /* Case : send a signal to me ( S(trx1, working) -> X(trx1, waiting) ) */
            if (Lock_Table[table_id].find(key) != Lock_Table[table_id].end()) {
                // if stmt means that there is only 1 working lock
                if (Lock_Table[table_id][key]->head->next != NULL &&
                    Lock_Table[table_id][key]->head->next->is_waiting == 1 &&
                    Lock_Table[table_id][key]->head->trx_id == Lock_Table[table_id][key]->head->next->trx_id) {

                    temp_lock_obj = Lock_Table[table_id][key]->head;

                    Lock_Table[table_id][key]->head = Lock_Table[table_id][key]->head->next;
                    Lock_Table[table_id][key]->head->prev = NULL;

                    trx_cut_linking(temp_lock_obj);
                    free(temp_lock_obj);

                    send_signal(Lock_Table[table_id][key]->head);
                }
            }

        }

        /* Case 2 : lock_obj is waiting */
        else {

            /* Case 2-1 : prev lock is working */
            if (lock_obj->prev->is_waiting == 0) {

                /* Case 2-1-1 : lock_obj is tail */
                if (Lock_Table[table_id][key]->tail == lock_obj) {
                    Lock_Table[table_id][key]->tail = lock_obj->prev;
                    lock_obj->prev->next = NULL;
                }

                /* Case 2-1-2 : lock_obj is not tail */
                else {

                    /* Case 2-1-2-1 : prev is S working and next is S waiting */
                    if (lock_obj->prev->lock_mode == 0 && lock_obj->next->lock_mode == 0) {
                        lock_obj->prev->next = lock_obj->next;
                        lock_obj->next->prev = lock_obj->prev;
                        send_signal(lock_obj->next);
                    }

                    /* Case 2-1-2-2 : Otherwise */
                    else {
                        lock_obj->prev->next = lock_obj->next;
                        lock_obj->next->prev = lock_obj->prev;
                    }

                }

            }

            /* Case 2-2 : prev lock is waiting */
            else {

                /* Case 2-2-1 : lock_obj is tail */
                if (Lock_Table[table_id][key]->tail == lock_obj) {
                    Lock_Table[table_id][key]->tail = lock_obj->prev;
                    lock_obj->prev->next = NULL;
                }

                /* Case 2-2-2 : lock_obj is not tail */
                else {
                    lock_obj->prev->next = lock_obj->next;
                    lock_obj->next->prev = lock_obj->prev;
                }

            }

        }

	}

	free(lock_obj);

	return 0;
}


void lock_wait(lock_t * lock_obj) {

    // Make it wait
    pthread_cond_wait(&lock_obj->cond, get_trx_latch(lock_obj->trx_id));

    // Release trx latch
    release_trx_latch(lock_obj->trx_id);

}


void lock_lock_latch() {
    pthread_mutex_lock(&Lock_Latch);
}


void release_lock_latch() {
    pthread_mutex_unlock(&Lock_Latch);
}


void print_lock_table() {
    unordered_map< int, unordered_map< int64_t, lock_table_entry * > >::iterator iter;
    unordered_map< int64_t, lock_table_entry * >::iterator iter1;
    lock_t * lock_obj;

    printf("Print Lock Table, Thread id : %u\n", pthread_self());

    for (iter = Lock_Table.begin(); iter != Lock_Table.end(); iter++) {
        for (iter1 = iter->second.begin(); iter1 != iter->second.end(); iter1++) {
            printf("Table id : %d, key : %d - ", iter->first, iter1->first);
            lock_obj = iter1->second->head;
            while (lock_obj != NULL) {
                printf("trx_id(%d), %d, %d -> ", lock_obj->trx_id, lock_obj->lock_mode, lock_obj->is_waiting);
                lock_obj = lock_obj->next;
            }
            printf("\n");
        }

    }
}

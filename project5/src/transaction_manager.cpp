#include "transaction_manager.h"
#include "index_manager.h"

int Global_Trx_Id = 0;
pthread_mutex_t Trx_Global_Latch;
pthread_mutex_t Trx_Table_Latch;
pthread_mutex_t Trx_Abort_Latch;

unordered_map< int, trx_t > Trx_Table;
unordered_map< int, int > is_abort;


int init_trx() {
    pthread_mutex_init(&Trx_Global_Latch, NULL);
    pthread_mutex_init(&Trx_Table_Latch, NULL);
    pthread_mutex_init(&Trx_Abort_Latch, NULL);
}


// User call
int trx_begin() {

    pthread_mutex_lock(&Trx_Global_Latch);

    trx_t trx;

    trx.trx_id = ++Global_Trx_Id;
    trx.next = NULL;
    trx.tail = NULL;

    pthread_mutex_lock(&Trx_Abort_Latch);
    is_abort[trx.trx_id] = 0;
    pthread_mutex_unlock(&Trx_Abort_Latch);

    pthread_mutex_lock(&Trx_Table_Latch);
    Trx_Table[trx.trx_id] = trx;
    pthread_mutex_unlock(&Trx_Table_Latch);

    pthread_mutex_unlock(&Trx_Global_Latch);

    return trx.trx_id;
}


// User call
int trx_commit(int trx_id) {

    lock_t * lock_obj, * free_lock_obj;

    pthread_mutex_lock(&Trx_Table_Latch);
    lock_obj = Trx_Table[trx_id].next;
    pthread_mutex_unlock(&Trx_Table_Latch);

    lock_get_latch();
    while (lock_obj != NULL) {
        free_lock_obj = lock_obj;
        lock_obj = lock_obj->trx_next;
        lock_release(free_lock_obj, 0);
    }
    lock_release_latch();

    pthread_mutex_lock(&Trx_Table_Latch);
    Trx_Table[trx_id].undo_log_map.clear();
    Trx_Table[trx_id].log_flag.clear();
    Trx_Table.erase(trx_id);
    pthread_mutex_unlock(&Trx_Table_Latch);

    return trx_id;
}


// DB API call
void trx_abort(int trx_id) {

    lock_t * lock_obj, * free_lock_obj;
    int lock_obj_table_id;
    int lock_obj_key;

    pthread_mutex_lock(&Trx_Table_Latch);
    lock_obj = Trx_Table[trx_id].next;
    pthread_mutex_unlock(&Trx_Table_Latch);

    lock_get_latch();
    while (lock_obj != NULL) {

        lock_obj_table_id = lock_obj->sentinel->table_id;
        lock_obj_key = lock_obj->sentinel->key;

        if (lock_obj->undo == 1)
            undo(lock_obj_table_id, lock_obj_key, Trx_Table[trx_id].undo_log_map[lock_obj_table_id][lock_obj_key].old_value);

        free_lock_obj = lock_obj;
        lock_obj = lock_obj->trx_next;
        lock_release(free_lock_obj, 1);
    }
    lock_release_latch();

    pthread_mutex_lock(&Trx_Abort_Latch);
    is_abort[trx_id] = 1;
    pthread_mutex_unlock(&Trx_Abort_Latch);

    pthread_mutex_lock(&Trx_Table_Latch);
    Trx_Table.erase(trx_id);
    pthread_mutex_unlock(&Trx_Table_Latch);
}


// Lock manager call
unordered_map< int, int > get_wait_for_list(lock_t * lock_obj, unordered_map< int, int > is_visit) {

    lock_t * target_lock_obj, * trx_lock_obj;
    unordered_map< int, int >::iterator iter;

    // Check only when lock_obj is waiting
    if (lock_obj->is_waiting == 1) {

        target_lock_obj = lock_obj->prev;

        /* Case 1 : target is S mode working lock
         * lock_acquire guarantees that lock_obj is X lock
         * */
        if (target_lock_obj->is_waiting == 0 && target_lock_obj->lock_mode == 0) {

            while (target_lock_obj != NULL) {
                /* trx1(S) - trx2(S) - trx1(X, lock_obj), skip checking trx1(S)
                 * because, logic guarantees that trx1 is not waiting for trx1
                 */
                if (target_lock_obj->trx_id != lock_obj->trx_id) {
                    if (is_visit.find(target_lock_obj->trx_id) == is_visit.end()) {
                        is_visit[target_lock_obj->trx_id] = 1;
                        trx_lock_obj = Trx_Table[target_lock_obj->trx_id].next;
                        while (trx_lock_obj != NULL) {
                            if (trx_lock_obj->is_waiting == 1)
                                is_visit = get_wait_for_list(trx_lock_obj, is_visit);
                            trx_lock_obj = trx_lock_obj->trx_next;
                        }

                    }
                }

                target_lock_obj = target_lock_obj->prev;
            }

        }

        /* Case 2 : target is X mode working lock */
        else if (target_lock_obj->is_waiting == 0 && target_lock_obj->lock_mode == 1) {
            if (is_visit.find(target_lock_obj->trx_id) == is_visit.end()) {
                is_visit[target_lock_obj->trx_id] = 1;
                trx_lock_obj = Trx_Table[target_lock_obj->trx_id].next;
                while (trx_lock_obj != NULL) {
                    if (trx_lock_obj->is_waiting == 1)
                        is_visit = get_wait_for_list(trx_lock_obj, is_visit);
                    trx_lock_obj = trx_lock_obj->trx_next;
                }
            }
        }

        /* Case 3 : target is S mode waiting lock */
        else if (target_lock_obj->is_waiting == 1 && target_lock_obj->lock_mode == 0) {

            // Finding wait for lock object
            while (target_lock_obj->is_waiting == 1) {
                if (target_lock_obj->lock_mode == 1)
                    break;
                target_lock_obj = target_lock_obj->prev;
            }

            /* Case : wait for lock is working lock */
            if (target_lock_obj->is_waiting == 0) {
                while (target_lock_obj != NULL) {
                    if (is_visit.find(target_lock_obj->trx_id) == is_visit.end()) {
                        is_visit[target_lock_obj->trx_id] = 1;
                        trx_lock_obj = Trx_Table[target_lock_obj->trx_id].next;
                        while (trx_lock_obj != NULL) {
                            if (trx_lock_obj->is_waiting == 1)
                                is_visit = get_wait_for_list(trx_lock_obj, is_visit);
                            trx_lock_obj = trx_lock_obj->trx_next;
                        }
                    }

                    target_lock_obj = target_lock_obj->prev;
                }
            }

            /* Case : wait for lock is X mode waiting lock */
            else {
                if (is_visit.find(target_lock_obj->trx_id) == is_visit.end()) {
                    is_visit[target_lock_obj->trx_id] = 1;
                    trx_lock_obj = Trx_Table[target_lock_obj->trx_id].next;
                    while (trx_lock_obj != NULL) {
                        if (trx_lock_obj->is_waiting == 1)
                            is_visit = get_wait_for_list(trx_lock_obj, is_visit);
                        trx_lock_obj = trx_lock_obj->trx_next;
                    }
                }
            }

        }

        /* Case 4 : target is X mode waiting lock */
        else if (target_lock_obj->is_waiting == 1 && target_lock_obj->lock_mode == 1) {
            if (is_visit.find(target_lock_obj->trx_id) == is_visit.end()) {
                is_visit[target_lock_obj->trx_id] = 1;
                trx_lock_obj = Trx_Table[target_lock_obj->trx_id].next;
                while (trx_lock_obj != NULL) {
                    if (trx_lock_obj->is_waiting == 1)
                        is_visit = get_wait_for_list(trx_lock_obj, is_visit);
                    trx_lock_obj = trx_lock_obj->trx_next;
                }
            }
        }

    }

    return is_visit;
}


// db_update call
void trx_logging(lock_t * lock_obj, char * old_value) {

    pthread_mutex_lock(&Trx_Table_Latch);

    unordered_map <int64_t, int> log_flag;
    unordered_map< int64_t, undoLog > undo_log_map_entry;
    undoLog undo_log;
    int table_id;
    int trx_id;
    int64_t key;

    table_id = lock_obj->sentinel->table_id;
    key = lock_obj->sentinel->key;
    trx_id = lock_obj->trx_id;

    if (Trx_Table[trx_id].log_flag.count(table_id) == 0) {

        // Make undo log
        undo_log.table_id = table_id;
        undo_log.key = key;
        strcpy(undo_log.old_value, old_value);

        // Push undo log to trx
        undo_log_map_entry[key] = undo_log;
        Trx_Table[trx_id].undo_log_map[table_id] = undo_log_map_entry;

        // Make log_flag
        log_flag[key] = 1;

        // Push log_flag to trx
        Trx_Table[trx_id].log_flag[table_id] = log_flag;

    }

    else {

        if (Trx_Table[trx_id].log_flag[table_id].count(key) == 0) {

            // Make undo log
            undo_log.table_id = table_id;
            undo_log.key = key;
            strcpy(undo_log.old_value, old_value);

            // Push undo log to trx
            Trx_Table[trx_id].undo_log_map[table_id][key] = undo_log;

            // Push log_flag to trx
            Trx_Table[trx_id].log_flag[table_id][key] = 1;

        }

    }

    pthread_mutex_unlock(&Trx_Table_Latch);
}


// lock manager call
void trx_linking(lock_t * lock_obj) {

    pthread_mutex_lock(&Trx_Table_Latch);

    int trx_id;
    trx_id = lock_obj->trx_id;

    if (Trx_Table[trx_id].next == NULL) {
        Trx_Table[trx_id].next = lock_obj;
        Trx_Table[trx_id].tail = lock_obj;
    }

    else {
        lock_obj->trx_prev = Trx_Table[trx_id].tail;
        Trx_Table[trx_id].tail->trx_next = lock_obj;
        Trx_Table[trx_id].tail = lock_obj;
    }

    pthread_mutex_unlock(&Trx_Table_Latch);

}


// lock manager call
void trx_cut_linking(lock_t * lock_obj) {

    pthread_mutex_lock(&Trx_Table_Latch);

    int trx_id;
    trx_id = lock_obj->trx_id;

    if (Trx_Table[trx_id].next == lock_obj && Trx_Table[trx_id].tail == lock_obj) {
        // This case should never happen
        printf(
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 10 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        );
    }

    else if (Trx_Table[trx_id].next == lock_obj && Trx_Table[trx_id].tail != lock_obj) {
        Trx_Table[trx_id].next = lock_obj->trx_next;
        Trx_Table[trx_id].next->trx_prev = NULL;
    }

    else if (Trx_Table[trx_id].next != lock_obj && Trx_Table[trx_id].tail == lock_obj) {
        // This case should never happen
        printf(
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ OHNONO ERROR 11 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        );
    }

    else if (Trx_Table[trx_id].next != lock_obj && Trx_Table[trx_id].tail != lock_obj) {
        lock_obj->trx_prev->trx_next = lock_obj->trx_next;
        lock_obj->trx_next->trx_prev = lock_obj->trx_prev;
    }

    pthread_mutex_unlock(&Trx_Table_Latch);

}


// DB API call
int trx_abort_check(int trx_id) {

    int abort_value;

    pthread_mutex_lock(&Trx_Abort_Latch);
    abort_value = is_abort[trx_id];
    pthread_mutex_unlock(&Trx_Abort_Latch);

    return abort_value;
}


void trx_get_latch() {
    pthread_mutex_lock(&Trx_Table_Latch);
}
void trx_release_latch() {
    pthread_mutex_unlock(&Trx_Table_Latch);
}

void print_Trx_Table() {
    lock_t * lock_obj;
    unordered_map< int, trx_t >::iterator iter1;
    int flag = 0;
    trx_t * temp = NULL, * temp2;

    printf("Print Trx Table, Thread id : %u\n", pthread_self());
    for (iter1 = Trx_Table.begin(); iter1 != Trx_Table.end(); iter1++) {
        printf("Trx %d (%p) - ", iter1->first, &iter1);
        lock_obj = iter1->second.next;

        while (lock_obj != NULL) {
            printf("(Table_id : %d, key : %d) -> ", lock_obj->sentinel->table_id, lock_obj->sentinel->key);
            lock_obj = lock_obj->trx_next;
        }
        printf("\n");

        if (temp == &Trx_Table[iter1->first]) {
            printf("head - (Table_id : %d, key : %d)\n",
                    Trx_Table[iter1->first].next->sentinel->table_id, Trx_Table[iter1->first].next->sentinel->key);
            printf("l\n");
        }

        temp = &Trx_Table[iter1->first];
    }

}

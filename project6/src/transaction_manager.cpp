#include "transaction_manager.h"
#include "index_manager.h"
#include "log_manager.h"

int Global_Trx_Id = 0;
pthread_mutex_t Trx_Global_Latch;
pthread_mutex_t Trx_Table_Latch;
pthread_mutex_t Trx_Abort_Latch;

unordered_map< int, trx_t > Trx_Table;
unordered_map< int, int > is_abort;


// return 0 : Success
// return 1 : Mutex error
int init_trx() {
    if (pthread_mutex_init(&Trx_Global_Latch, NULL) != 0) return 1;
    if (pthread_mutex_init(&Trx_Table_Latch, NULL) != 0) return 1;
    if (pthread_mutex_init(&Trx_Abort_Latch, NULL) != 0) return 1;
}


// User call
int trx_begin() {

    pthread_mutex_lock(&Trx_Global_Latch);

    trx_t trx;

    trx.trx_id = ++Global_Trx_Id;
    trx.next = NULL;
    trx.tail = NULL;
    pthread_mutex_init(&trx.trx_latch, NULL);

    pthread_mutex_lock(&Trx_Abort_Latch);
    is_abort[trx.trx_id] = 0;
    pthread_mutex_unlock(&Trx_Abort_Latch);

    pthread_mutex_lock(&Trx_Table_Latch);
    Trx_Table[trx.trx_id] = trx;
    pthread_mutex_unlock(&Trx_Table_Latch);

    issue_begin_log(trx.trx_id);

    pthread_mutex_unlock(&Trx_Global_Latch);

    return trx.trx_id;
}


// User call
int trx_commit(int trx_id) {

    lock_t * lock_obj, * free_lock_obj;

    pthread_mutex_lock(&Trx_Table_Latch);
    lock_obj = Trx_Table[trx_id].next;
    pthread_mutex_unlock(&Trx_Table_Latch);

    lock_lock_latch();

    while (lock_obj != NULL) {
        free_lock_obj = lock_obj;
        lock_obj = lock_obj->trx_next;
        lock_release(free_lock_obj, 0);
    }

    release_lock_latch();

    pthread_mutex_lock(&Trx_Table_Latch);
    Trx_Table.erase(trx_id);
    pthread_mutex_unlock(&Trx_Table_Latch);

    issue_commit_log(trx_id);

    write_log(0, 0);

    return trx_id;
}


// DB API call
void trx_abort(int trx_id) {

    lock_t * lock_obj, * free_lock_obj;
    int next_undo_LSN;
    int compensate_lsn;

    vector< undoLog >   undo_log_list;
    undoLog             undo_log;

    pthread_mutex_lock(&Trx_Table_Latch);
    lock_obj = Trx_Table[trx_id].next;
    undo_log_list = Trx_Table[trx_id].undo_log_list;
    pthread_mutex_unlock(&Trx_Table_Latch);

    /* ------------------------------ UNDO ------------------------------ */

    while (undo_log_list.size() != 0) {

        undo_log = undo_log_list.back();
        undo_log_list.pop_back();

        if (undo_log_list.size() == 0)  next_undo_LSN = -1;
        else                            next_undo_LSN = undo_log_list.back().lsn;

        compensate_lsn = issue_compensate_log(trx_id, undo_log.table_id, undo_log.pagenum,
                                              (undo_log.pagenum * PAGE_SIZE) + PAGE_HEADER_SIZE + (128 * undo_log.key_index),
                                              undo_log.old_value, undo_log.new_value, next_undo_LSN);

        undo(undo_log.table_id, undo_log.pagenum, undo_log.key, undo_log.old_value, compensate_lsn);

    }
    /* ------------------------------------------------------------------ */


    /* ----------------------------- RELEASE ---------------------------- */
    lock_lock_latch();

    // Release lock from back to forward to do correct undo
    while (lock_obj != NULL) {
        free_lock_obj = lock_obj;
        lock_obj = lock_obj->trx_next;
        lock_release(free_lock_obj, 1);
    }

    release_lock_latch();
    /* ------------------------------------------------------------------ */

    pthread_mutex_lock(&Trx_Abort_Latch);
    is_abort[trx_id] = 1;
    pthread_mutex_unlock(&Trx_Abort_Latch);

    pthread_mutex_lock(&Trx_Table_Latch);
    Trx_Table.erase(trx_id);
    pthread_mutex_unlock(&Trx_Table_Latch);

    // Issue a ROLLBACK Log
    issue_rollback_log(trx_id);

    write_log(0, 0);

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
void trx_logging(int table_id, int key, int trx_id, int64_t lsn, int64_t pagenum, int key_index,
        char * old_value, char * new_value) {

    undoLog undo_log;

    // Set undo log
    undo_log.lsn = lsn;
    undo_log.table_id = table_id;
    undo_log.pagenum = pagenum;
    undo_log.key = key;
    undo_log.key_index = key_index;
    strcpy(undo_log.old_value, old_value);
    strcpy(undo_log.new_value, new_value);

    // Push undo log to trx undo log list
    pthread_mutex_lock(&Trx_Table_Latch);
    Trx_Table[trx_id].undo_log_list.push_back(undo_log);
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


void lock_trx_table_latch() {
    pthread_mutex_lock(&Trx_Table_Latch);
}


void release_trx_table_latch() {
    pthread_mutex_unlock(&Trx_Table_Latch);
}


void lock_trx_latch(int trx_id) {
    pthread_mutex_lock(&Trx_Table_Latch);
    pthread_mutex_lock(&Trx_Table[trx_id].trx_latch);
    pthread_mutex_unlock(&Trx_Table_Latch);
}


void release_trx_latch(int trx_id) {
    pthread_mutex_lock(&Trx_Table_Latch);
    pthread_mutex_unlock(&Trx_Table[trx_id].trx_latch);
    pthread_mutex_unlock(&Trx_Table_Latch);
}


pthread_mutex_t * get_trx_latch(int trx_id) {
    pthread_mutex_t * trx_latch;
    pthread_mutex_lock(&Trx_Table_Latch);
    trx_latch = &Trx_Table[trx_id].trx_latch;
    pthread_mutex_unlock(&Trx_Table_Latch);
    return trx_latch;
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

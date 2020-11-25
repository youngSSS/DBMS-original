#include "transaction_manager.h"
#include "index_manager.h"

int Global_Trx_Id = 0;
pthread_mutex_t Trx_Latch = PTHREAD_MUTEX_INITIALIZER;

unordered_map< int, trx_t > Trx_Table;

int trx_begin() {

    pthread_mutex_lock(&Trx_Latch);

    trx_t trx;

    trx.trx_id = ++Global_Trx_Id;
    trx.next = NULL;
    trx.is_aborted = 0;
    
    Trx_Table[trx.trx_id] = trx;

    pthread_mutex_unlock(&Trx_Latch);

    return trx.trx_id;
}


int trx_commit(int trx_id) {

    pthread_mutex_lock(&Trx_Latch);

    lock_t * lock_obj, * free_lock_obj;

    lock_obj = Trx_Table[trx_id].next;

    while (lock_obj != NULL) {

        free_lock_obj = lock_obj;

        lock_obj = lock_obj->trx_next;

        lock_release(free_lock_obj, 0);
        
    }

    Trx_Table.erase(trx_id);

    pthread_mutex_unlock(&Trx_Latch);

    return trx_id;
}


void trx_abort(int trx_id) {

    pthread_mutex_lock(&Trx_Latch);

	lock_t * lock_obj, * free_lock_obj;
    int lock_obj_table_id;
    int lock_obj_key;

    lock_obj = Trx_Table[trx_id].next;

    while (lock_obj != NULL) {

        lock_obj_table_id = lock_obj->sentinel->table_id;
        lock_obj_key = lock_obj->sentinel->key;

        if (Trx_Table[trx_id].undo_log_map[lock_obj_table_id].find(lock_obj_key) != Trx_Table[trx_id].undo_log_map[lock_obj_table_id].end()) {
            undo(lock_obj_table_id, lock_obj_key, Trx_Table[trx_id].undo_log_map[lock_obj_table_id][lock_obj_key].old_value);
        }

        free_lock_obj = lock_obj;

        lock_obj = lock_obj->trx_next;

        lock_release(free_lock_obj, 1);

    }

    Trx_Table.erase(trx_id);

    pthread_mutex_unlock(&Trx_Latch);
    pthread_mutex_unlock(&Lock_Latch);

}


unordered_map< int, int > get_wait_for_list(unordered_map< int, int > wait_for_list, int trx_id) {
    unordered_map< int, int > wait_for, temp_wait_for;
    unordered_map< int, int >::iterator iter;
    lock_t * trx_lock_obj, * lock_obj;

    trx_lock_obj = Trx_Table[trx_id].next;

    while (trx_lock_obj != NULL) {

        if (wait_for_list.find(trx_lock_obj->trx_id) == wait_for_list.end()) {

            lock_obj = trx_lock_obj;

            if (lock_obj->is_waiting == 1) {

                if (lock_obj->prev->is_waiting == 0) {

                    lock_obj = lock_obj->prev;

                    while (lock_obj != NULL) {
                        wait_for[lock_obj->trx_id] = 1;
                        lock_obj = lock_obj->prev;
                    }

                }

                else {

                    if (lock_obj->prev->lock_mode == 0) {
                        
                        lock_obj = lock_obj->prev;

                        while (lock_obj->lock_mode == 0) {
                            temp_wait_for = get_wait_for_list(wait_for, lock_obj->trx_id);

                            for (iter = temp_wait_for.begin(); iter != temp_wait_for.end(); iter++)
                                wait_for[iter->first] = iter->second;

                            wait_for[lock_obj->trx_id] = 1;
                            lock_obj = lock_obj->prev;
                        }
                    }

                    else {
                        temp_wait_for = get_wait_for_list(wait_for, lock_obj->trx_id);

                        for (iter = temp_wait_for.begin(); iter != temp_wait_for.end(); iter++)
                            wait_for[iter->first] = iter->second;

                        wait_for[lock_obj->prev->trx_id] = 1;
                    }

                }
            }

        }

        trx_lock_obj = trx_lock_obj->trx_next;
    }

    wait_for[trx_id] = 1;

    printf("Wait for list : ");
    for (iter = wait_for.begin(); iter != wait_for.end(); iter++)
        printf("%d ", iter->first);
    printf(", Thread id : %u\n", pthread_self());

    return wait_for;
}


int trx_abort_check(int trx_id) {
    return Trx_Table[trx_id].is_aborted;
}


void trx_logging(int table_id, int64_t key, char * old_value, int trx_id) {
    if (Trx_Table[trx_id].undo_log_map[table_id].find(key) == Trx_Table[trx_id].undo_log_map[table_id].end()) {
        undoLog undo_log;
        undo_log.table_id = table_id;
        undo_log.key = key;
        strcpy(undo_log.old_value, old_value);

        Trx_Table[trx_id].undo_log_map[table_id][key] = undo_log;
    }

}


void trx_linking(lock_t * lock_obj, int trx_id) {

    if (Trx_Table[trx_id].next == NULL) {
        Trx_Table[trx_id].next = lock_obj;
        Trx_Table[trx_id].tail = lock_obj;
    }

    else {
        Trx_Table[trx_id].tail->trx_next = lock_obj;
        Trx_Table[trx_id].tail = lock_obj;
    }   
    
}

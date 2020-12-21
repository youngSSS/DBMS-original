#ifndef __TRANSACTION_MANAGER_H__
#define __TRANSACTION_MANAGER_H__

#include "lock_manager.h"

using namespace std;

typedef struct trx_t trx_t;
typedef struct undoLog undoLog;

struct undoLog {
    int64_t lsn;
    int table_id;
    int64_t pagenum;
    int64_t key;
    int key_index;
    char old_value[120];
    char new_value[120];
};

struct trx_t {
    int trx_id;
    lock_t * next;
    lock_t * tail;
    vector< undoLog > undo_log_list;
    pthread_mutex_t trx_latch;
};



/* APIs for transaction manager */

// Transaction Function
int init_trx();
int trx_begin();
int trx_commit(int trx_id);
void trx_abort(int trx_id);

// Deadlock Detect Function
unordered_map< int, int > get_wait_for_list(lock_t * lock_obj, unordered_map< int, int > is_visit);

// Issue a log to the transaction for abort
void trx_logging(int table_id, int key, int trx_id, int64_t lsn, int64_t pagenum, int key_index,
                 char * old_value, char * new_value);
// Trx_Table Function
void trx_linking(lock_t * lock_obj);
void trx_cut_linking(lock_t * lock_obj);

// Transaction abort check
int trx_abort_check(int trx_id);

// To access to Trx_Table latch from other layer
void lock_trx_table_latch();
void release_trx_table_latch();

// To access to trx latch from other layer
void lock_trx_latch(int trx_id);
void release_trx_latch(int trx_id);

// To release trx latch after waiting
pthread_mutex_t  * get_trx_latch(int trx_id);

void print_Trx_Table();

#endif /* __TRANSACTION_MANAGER_H__ */

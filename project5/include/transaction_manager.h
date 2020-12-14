#ifndef __TRANSACTION_MANAGER_H__
#define __TRANSACTION_MANAGER_H__

#include "lock_manager.h"

using namespace std;

typedef struct trx_t trx_t;
typedef struct undoLog undoLog;

struct undoLog {
    int table_id;
    int64_t key;
    char old_value[120];
};

struct trx_t {
	int trx_id;
	lock_t * next;
	lock_t * tail;
	unordered_map< int, unordered_map< int64_t, undoLog > > undo_log_map;
    unordered_map< int, unordered_map< int64_t, int > > log_flag;
};


/* APIs for transaction manager */
int init_trx();
int trx_begin();
int trx_commit(int trx_id);
void trx_abort(int trx_id);
unordered_map< int, int > get_wait_for_list(lock_t * lock_obj, unordered_map< int, int > is_visit);
void trx_logging(lock_t * lock_obj, char * old_value);
void trx_linking(lock_t * lock_obj);
void trx_cut_linking(lock_t * lock_obj);
int trx_abort_check(int trx_id);
void trx_get_latch();
void trx_release_latch();
void print_Trx_Table();

#endif /* __TRANSACTION_MANAGER_H__ */

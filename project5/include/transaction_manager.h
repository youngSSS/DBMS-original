#ifndef __TRANSACTION_MANAGER_H__
#define __TRANSACTION_MANAGER_H__

#include "lock_manager.h"

using namespace std;

typedef struct trx_t trx_t;
typedef struct undoLog undoLog;

extern pthread_mutex_t Lock_Latch;


struct trx_t {
	int trx_id;
	lock_t * next;
	lock_t * tail;
	unordered_map< int, int > wait_for_list;
	int is_aborted;
	unordered_map< int, unordered_map< int, undoLog > > undo_log_map;
};

struct undoLog {
	int table_id;
	int64_t key;
	char old_value[120];
};


/* APIs for transaction manager */
int trx_begin();
int trx_commit(int trx_id);
void trx_abort(int trx_id);
unordered_map< int, int > get_wait_for_list(unordered_map< int, int > wait_for_list, int trx_id);
int trx_abort_check(int trx_id);
void trx_logging(int table_id, int64_t key, char * old_value, int trx_id);
void trx_linking(lock_t * lock_obj, int trx_id);

#endif /* __TRANSACTION_MANAGER_H__ */

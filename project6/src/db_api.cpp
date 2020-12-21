/* DB API */

#include "db_api.h"
#include "index_manager.h"
#include "transaction_manager.h"
#include "log_manager.h"


/* ---------- Main APIs ---------- */

// return 0 : initializing success
// return 1 : making buffer error (malloc)
// return 2 : init_db is already called
// return 3 : buffer size must be over 0
// return 4 : lock table initialize error
// return 5 : transaction mutex error
// return 6 : log mutex error
// return 7 : recovery error

int init_db(int buf_num, int flag, int log_num, char * log_path, char * logmsg_path) {
    int result;

    // Buffer size must over 0
    if (buf_num <= 0) return 3;

    // DB initialize
    result = index_init_db(buf_num);
    if (result == 1) return result;

    result = init_trx();

    result = init_log(logmsg_path);

    // Recovery
    result = DB_recovery(flag, log_num, log_path);
    if (result != 0) return 7;

    return 0;
}


// return -2 : no more than 10 file
// return -1 : fail to open file
// return 0 : open success

int open_table(char * pathname) {
	return index_open(pathname);
}


// return 0 : insert success
// return 1 : file having table_id is not exist
// return 2 : duplicate key

int db_insert(int table_id, int64_t key, char * value) {
    int result;

    if (table_id > 10) return 1;
    if (get_is_open(table_id) == 0) return 1;

    result = insert(table_id, key, value);
    
    return result;
}


// return 0 : no matter to next operation
// return 1 : abort or had been aborted

int db_find(int table_id, int64_t key, char * ret_val, int trx_id) {
    int result;

    if (trx_abort_check(trx_id) == 1) {
//        printf("db_api - abort check\n");
        return 1;
    }


    if (table_id > 10) {
//        printf("db_api - table_id\n");
        return 1;
    }


    if (get_is_open(table_id) == 0) {
//        printf("db_api - is open\n");
        return 1;
    }


    result = trx_find(table_id, key, ret_val, trx_id);

    /* Case : Abort */
    if (result == 2) {
//        printf("db_api - abort\n");
        trx_abort(trx_id);
        return 1;
    }

    else if (result == 1) {
//        printf("No key\n");
        trx_abort(trx_id);
        return 1;
    }

    return 0;
}


// return 0 : delete success
// return 1 : file having table_id is not exist
// return 2 : no key

int db_delete(int table_id, int64_t key) {
    int result;

    if (table_id > 10) return 1;
    if (get_is_open(table_id) == 0) return 1;

    result = _delete(table_id, key);

    return result;
}


// return 0 : no matter to next operation
// return 1 : abort or had been aborted

int db_update(int table_id, int64_t key, char * values, int trx_id) {
    int result;

    if (trx_abort_check(trx_id) == 1)
        return 1;

    if (table_id > 10)
        return 1;

    if (get_is_open(table_id) == 0)
        return 1;

    result = trx_update(table_id, key, values, trx_id);

    /* Case : Abort */
    if (result == 2) {
        trx_abort(trx_id);
        return 1;
    }

    else if (result == 1) {
        trx_abort(trx_id);
        return 1;
    }

    return 0;
}


// return 0 : close success
// return 1 : file having table_id is not exist

int close_table(int table_id) {
    int result;

    if (table_id > 10) return 1;
    if (get_is_open(table_id) == 0) return 1;

    result = index_close(table_id);

    return result;
}


// return 0 : shutdown success

int shutdown_db( void ) {
    return index_shutdown_db();
}


/* ---------- Etc APIs ---------- */


void db_flush(int table_id) {
    if (table_id > 10) return;
    index_flush(table_id);
}


void db_print(int table_id) {
    if (table_id > 10) return;
    if (Is_open[table_id] == 0) return;
    print_file(table_id);
}


void db_print_leaf(int table_id) {
    if (table_id > 10) return;
    if (Is_open[table_id] == 0) return;
    print_leaf(table_id);
}

void db_print_table_list() {
	index_print_table_list();
}


void print_usage() {
    printf(
    "Enter any of the following commands after the prompt > :\n"
    "\t----------------------- Start -----------------------\n"
    "\tB <num>  -- Make buffer with size <num>\n"
    "\tO <pathname>  -- Oepn <pathname> file\n"
    "\tR <test_cnt> \n"
    "\t\tfor each test insert <flag> <log_num> <log_path> <logmsg_path>  -- DB Recovery test\n\n"
    "\t-------------------- Modification -------------------\n"
    "\ti <table_id> <key> <value>  -- Insert <key> <value>\n"
    "\tf <table_id> <key>  -- Find the value under <key>\n"
    "\td <table_id> <key>  -- Delete key <key> and its associated value\n"
    "\tI <table_id> <num1> <num2>  -- Insert <num1> ~ <num2>\n"
    "\tD <table_id> <num1> <num2>  -- Delete <num1> ~ <num2>\n\n"
    "\t-------------------- Transaction --------------------\n"
    "\tT  -- Operate Transaction Routine\n"
    "\t----------------------- Print -----------------------\n"
    "\tt  -- Print table id list\n"
    "\tp <table_id>  -- Print the data file in B+ tree structure\n"
    "\tl <table_id>  -- Print all leaf records\n\n"
    "\t-------------------- Termination --------------------\n"
    "\tC <table_id>  -- Close file having <table_id>\n"
    "\tS  -- Shutdown DB\n"
    "\tQ  -- Quit\n\n"
    "\t------------------------ Ect ------------------------\n"
    "\tF <table_id>  -- Flush data file from buffer to disk\n"
    "\tU  -- Print usage\n"
    );
}


/* ------ Help Functions ------ */

int get_is_open(int table_id){
    return index_is_open(table_id);
}
/* DB API */

#include "db_api.h"
#include "index_manager.h"


/* ---------- Main APIs ---------- */

// return 0 : initializing success
// return 1 : making buffer error (malloc)
// return 2 : init_db is already called
// return 3 : buffer size must be over 0

int init_db(int buf_num) {
    if (buf_num <= 0) return 3;
    return index_init_db(buf_num);
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


// return 0 : find success
// return 1 : file having table_id is not exist
// return 2 : no key

int db_find(int table_id, int64_t key, char * ret_val) {
    int result;

    if (table_id > 10) return 1;
    if (get_is_open(table_id) == 0) return 1;

    return _find(table_id, key, ret_val);
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


// return 0 : close succes
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
    "\tO <pathname>  -- Oepn <pathname> file\n\n"
    "\t-------------------- Modification -------------------\n"
    "\ti <table_id> <key> <value>  -- Insert <key> <value>\n"
    "\tf <table_id> <key>  -- Find the value under <key>\n"
    "\td <table_id> <key>  -- Delete key <key> and its associated value\n"
    "\tI <table_id> <num1> <num2>  -- Insert <num1> ~ <num2>\n"
    "\tD <table_id> <num1> <num2>  -- Delete <num1> ~ <num2>\n\n"
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
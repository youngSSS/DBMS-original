/* DB API */

#include "db_api.h"
#include "index_manager.h"

/* ------ APIs for Buffer ------ */


int init_db(int buf_num) {
    return index_init_buffer(buf_num);
}


int shutdown_db( void ) {
    return index_shutdown_buffer();
}


/* - APIs for File Modification - */


int open_table(char * pathname) {
	return index_open(pathname);
}

// return 0 : insert success
// return 1 : file is not opened
// return 2 : duplicate key

int db_insert(int table_id, int64_t key, char * value) {
    int result;

    if (is_opened(table_id) == 0) return 1;

    result = insert(table_id, key, value);
    
    return result;
}


// return 0 : find success
// return 1 : file is not opened
// return 2 : no key

int db_find(int table_id, int64_t key, char * ret_val) {
	leafRecord* leaf_record;

    if (is_opened(table_id) == 0) return 1;

    leaf_record = find(table_id, key);
    if (leaf_record == NULL) return 2;
    
    strcpy(ret_val, leaf_record->value);
    
    return 0;
}


// return 0 : delete success
// return 1 : file is not opened
// return 2 : no key

int db_delete(int table_id, int64_t key) {
    int result;

    if (is_opened(table_id) == 0) return 1;

    result = _delete(table_id, key);

    return result;
}

// return 0 : close succes
// return 1 : file having table_id is not exist

int close_table(int table_id) {
    int result;

    result = index_close(table_id);

    return result;
}


/* ------ Help Functions ------ */

void db_print(int table_id) {
    if (is_opened(table_id) == 0) return;
    print_file(table_id);
}


void db_print_leaf(int table_id) {
    if (is_opened(table_id) == 0) return;
    print_leaf(table_id);
}


int is_opened(int table_id) {

    /* Case : file having table_id is not opened */

    if (Table_id_pathname.find(table_id) == Table_id_pathname.end())
        return 0;

    /* Case : file having table_id is opened */

    else 
        return 1;
}
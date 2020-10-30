/* DB API */

#include "bpt.h"
#include "file.h"
#include "db_api.h"

// Global
int Unique_table_id = -1;


int open_table(char * pathname) {
	return index_open(pathname);
}

// return 0 : insert success
// return 1 : file is not opened
// return 2 : duplicate key

int db_insert(int64_t key, char * value) {
    int result;

    if (Unique_table_id < 0) return 1;

    result = insert(key, value);
    
    return result;
}

// return 0 : delete success
// return 1 : file is not opened
// return 2 : no key

int db_delete(int64_t key) {
    int result;

    if (Unique_table_id < 0) return 1;

    result = delete(key);

    return result;
}

// return 0 : find success
// return 1 : file is not opened
// return 2 : no key

int db_find(int64_t key, char * ret_val) {
	leafRecord* leaf_record;

    if (Unique_table_id < 0) return 1;

    leaf_record = find(key);
    if (leaf_record == NULL) return 2;
    
    strcpy(ret_val, leaf_record->value);
    
    return 0;
}


int db_close(int table_id) {
    int result;

    result = index_close(table_id);

    if (result == 0) {
        Unique_table_id = -1;
        return result;
    }
    else 
        return result;
}


void db_print() {
    if (Unique_table_id < 0) return;
    print_file();
}

void db_print_leaf() {
    if (Unique_table_id < 0) return;
    print_leaf();
}
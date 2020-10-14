/* DB API */

#include "bpt.h"
#include "file.h"
#include "db_api.h"

// Global
int Unique_table_id;


int open_table(char * pathname) {
	int file_num;
    int file_size;

	Unique_table_id = index_open(pathname);

    if (Unique_table_id < 0) 
        return Unique_table_id;

    file_size = index_check_file_size(Unique_table_id);

    header_page = (page_t*)malloc(sizeof(page_t));

    /* Case : File is empty */

    // Create header page
    if (file_size == 0) {
        header_page->h.free_pagenum = 0;
        header_page->h.root_pagenum = 0;
        header_page->h.num_pages = 1;

        file_write_page(0, header_page);
    }

    /* Case : File is not empty */

    //Copy an on-disk header page to in-memory header page.
    else 
        file_read_page(0, header_page);

	return Unique_table_id;
}


int db_insert(int64_t key, char * value) {
    int result;
    result = insert(key, value);
    
    return result;
}


int db_delete(int64_t key) {
    int result;
    result = delete(key);

    return result;
}


int db_find(int64_t key, char * ret_val) {
	leafRecord* leaf_record;
    leaf_record = find(key);
    if (leaf_record == NULL) return -1;
    
    strcpy(ret_val, leaf_record->value);
    
    return 0;
}


void db_print() {
    print_file();
}

void db_print_leaf() {
    print_leaf();
}
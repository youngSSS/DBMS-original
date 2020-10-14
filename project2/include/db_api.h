#ifndef __DB_API_H__
#define __DB_API_H__

#include <stdlib.h>

#include "file.h"

extern int Unique_table_id;
extern page_t * header_page;

/* DB API Functions */

int open_table(char * pathname);
int db_insert(int64_t key, char * value);
int db_delete(int64_t key);
int db_find(int64_t key, char * ret_val);
void db_print();
void db_print_leaf();

#endif /* __DB_API_H__*/
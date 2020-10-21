#ifndef __DB_API_H__
#define __DB_API_H__

#include <stdlib.h>

#include "file.h"

// Positioned in file.cpp
extern map<int, string> Table_id_pathname;
extern map<string, int> Pathname_table_id;

using namespace std;


/* ----- DB API Functions ----- */

// APIs for Buffer

int init_db(int buf_num);
int shutdown_db( void );

// APIs for File

int open_table(char * pathname);
int db_insert(int table_id, int64_t key, char * value);
int db_find(int table_id, int64_t key, char * ret_val);
int db_delete(int table_id, int64_t key);
int close_table(int table_id);

// Help Functions

void db_print(int table_id);
void db_print_leaf(int table_id);
int is_opened(int table_id);

#endif /* __DB_API_H__*/
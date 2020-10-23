#ifndef __DB_API_H__
#define __DB_API_H__

#include <stdlib.h>

#include "file.h"

// Positioned in file.cpp
extern uint8_t Is_open[11];

using namespace std;


/* ----- DB API Functions ----- */

int init_db(int buf_num);
int open_table(char * pathname);
int db_insert(int table_id, int64_t key, char * value);
int db_find(int table_id, int64_t key, char * ret_val);
int db_delete(int table_id, int64_t key);
int close_table(int table_id);
int shutdown_db( void );

// Help Functions

void db_print(int table_id);
void db_print_leaf(int table_id);
void db_print_table_list();

#endif /* __DB_API_H__*/
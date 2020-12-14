#ifndef __DB_API_H__
#define __DB_API_H__

#include <stdlib.h>
#include <pthread.h>

#include "file.h"

// Positioned in file.cpp
extern uint8_t Is_open[11];

using namespace std;


/* ----- DB API Functions ----- */

// Main APIs

int init_db(int buf_num, int flag, int log_num, char * log_path, char * logmsg_path);
int open_table(char * pathname);
int db_insert(int table_id, int64_t key, char * value);
int db_find(int table_id, int64_t key, char * ret_val, int trx_id);
int db_delete(int table_id, int64_t key);
int db_update(int table_id, int64_t key, char * value, int trx_id);
int close_table(int table_id);
int shutdown_db( void );

// Etc APIs

void db_flush(int table_id);
void db_print(int table_id);
void db_print_leaf(int table_id);
void db_print_table_list();
void print_usage();

/* ------ Help Functions ------ */

int get_is_open(int table_id);


#endif /* __DB_API_H__*/
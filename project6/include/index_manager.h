#ifndef __INDEX_MANAGER_H__
#define __INDEX_MANAGER_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <vector>

#include "file.h"

// Max size of queue
#define MAX 100000000

using namespace std;
using p_pnum = pair<page_t *, pagenum_t>;


// Queue

int IsEmpty();
int IsFull();
void enqueue(pagenum_t pagenum);
pagenum_t dequeue();


/* --- For layered architecture --- */

// Buffer

int index_init_db(int buf_num);
int index_shutdown_db( void );
void index_flush(int table_id);

// File

int index_open(char * pathname);
int index_close(int table_id);
int index_is_open(int table_id);
void index_print_table_list();


/* ------- Transaction APIs ------- */

int trx_find(int table_id, int64_t key, char * ret_val, int trx_id);
int trx_update(int table_id, int64_t key, char * values, int trx_id);
int undo(int table_id, pagenum_t pagenum, int64_t key, char * old_value, int compensate_lsn);


/* ---------- Index APIs ---------- */

// Print

void print_leaf(int table_id);
void print_file(int table_id);

// Find

p_pnum find_leaf_page(int table_id, pagenum_t root_pagenum, int64_t key);
leafRecord * find(int table_id, pagenum_t root_pagenum, int64_t key);
int _find(int table_id, int64_t key, char * ret_val);

// Insertion

leafRecord make_leaf_record(int64_t key, char* value);
page_t * make_page( void );
page_t * make_leaf_pgae( void );
int get_left_index(int table_id, page_t * parent, pagenum_t left_pagenum);
void insert_into_leaf(int table_id, p_pnum leaf_pair, int64_t key, leafRecord leaf_record);
int insert_into_leaf_after_splitting(int table_id, p_pnum leaf_pair, int64_t key, leafRecord leaf_record);
int insert_into_page(int table_id, p_pnum parent_pair, int left_index, int64_t key, pagenum_t right_pagenum);
int insert_into_page_after_splitting(int table_id, p_pnum old_pair, int left_index, int64_t key, pagenum_t right_pagenum);
int insert_into_parent(int table_id, pagenum_t parent_pagenum, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
int insert_into_new_root(int table_id, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
int start_new_tree(int table_id, int64_t key, leafRecord leaf_record);
int insert(int table_id, int64_t key, char* value);

// Deletion

page_t * remove_entry_from_page(int table_id, p_pnum page_pair, int key_index);
int adjust_root(int table_id, p_pnum root_pair);
int coalesce_nodes(int table_id, page_t * parent, p_pnum key_pair, p_pnum neighbor_pair, int neighbor_index, int k_prime);
int redistribute_nodes(int table_id, page_t * parent, p_pnum key_pair, p_pnum neighbor_pair, int neighbor_flag
    , int k_prime_index, int k_prime);
int get_neighbor_index(int table_id, page_t * parent, pagenum_t key_pagenum);
int delete_entry(int table_id, p_pnum key_pair, int key_index);
int _delete(int table_id, int64_t key);

// Etc

int cut(int length);

#endif /* __INDEX_MANAGER_H__*/
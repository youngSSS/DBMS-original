#ifndef __INDEX_MANAGER_H__
#define __INDEX_MANAGER_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include "file.h"

// Max size of queue
#define MAX 100000000

using namespace std;


// Queue

int IsEmpty();
int IsFull();
void enqueue(pagenum_t pagenum);
pagenum_t dequeue();


/* --- For layered architecture --- */

// Buffer

int index_init_buffer(int buf_num);
int index_shutdown_buffer( void );

// File

int index_open(char * pathname);
int index_close(int table_id);


/* ---------- Index APIs ---------- */

// Print

void print_leaf(int table_id);
void print_file(int table_id);

// Find

page_t * find_leaf_page(int table_id, int64_t key);
leafRecord * find(int table_id, int64_t key);

// Insertion

leafRecord make_leaf_record(int64_t key, char* value);
page_t * make_page( void );
page_t * make_leaf_pgae( void );
int get_left_index(int table_id, page_t * parent, page_t * left);
void insert_into_leaf(int table_id, page_t* leaf_page, int64_t key, leafRecord leaf_record);
int insert_into_leaf_after_splitting(int table_id, page_t * leaf_page, int64_t key, leafRecord leaf_record);
int insert_into_page(int table_id, page_t * parent, int left_index, int64_t key, pagenum_t right_pagenum);
int insert_into_page_after_splitting(int table_id, page_t * old_page, int left_index, int64_t key, pagenum_t right_pagenum);
int insert_into_parent(int table_id, page_t * left, int64_t key, page_t * right, pagenum_t right_pagenum);
int insert_into_new_root(int table_id, page_t * left, int64_t key, page_t * right, pagenum_t right_pagenum);
int start_new_tree(int table_id, int64_t key, leafRecord leaf_record);
int insert(int table_id, int64_t key, char* value);

// Deletion

page_t * remove_entry_from_page(int table_id, page_t * page, int key_index);
int adjust_root(int table_id, page_t * root);
int coalesce_nodes(int table_id, page_t * parent, page_t * key_page, page_t * neighbor, int neighbor_flag, int k_prime);
int redistribute_nodes(int table_id, page_t * parent_page, page_t * key_page, page_t * neighbor, 
			int neighbor_flag, int k_prime_index, int k_prime);
int get_neighbor_index(int table_id, page_t * parent, page_t * key_page);
int delete_entry(int table_id, page_t * key_page, int key_index);
int _delete(int table_id, int64_t key);

// Etc

pagenum_t get_pagenum(int table_id, page_t* page);
int cut(int length);

#endif /* __INDEX_MANAGER_H__*/
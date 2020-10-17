#ifndef __BPT_H__
#define __BPT_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include "file.h"

// Max size of queue
#define MAX 100000000

extern page_t * header_page;

// Queue

int IsEmpty();
int IsFull();
void enqueue(pagenum_t pagenum);
pagenum_t dequeue();

// Open

int index_open(char * pathname);

// Close

int index_close(int table_id);

// Check file size

int index_check_file_size(int unique_table_id);

// Print

void print_leaf();
void print_file();
void find_and_print(int64_t key);

// Find

page_t * find_leaf_page(int64_t key);
leafRecord * find(int64_t key);

// Insertion.

leafRecord make_leaf_record(int64_t key, char* value);
page_t * make_page( void );
page_t * make_leaf_pgae( void );
int get_left_index(page_t * parent, page_t * left);
int insert_into_leaf( page_t* leaf_page, int64_t key, leafRecord leaf_record );
int insert_into_leaf_after_splitting(page_t * leaf_page, int64_t key, leafRecord leaf_record);
int insert_into_page(page_t * parent, int left_index, int64_t key, pagenum_t right_pagenum);
int insert_into_page_after_splitting(page_t * old_page, int left_index, int64_t key, pagenum_t right_pagenum);
int insert_into_parent(page_t * left, int64_t key, page_t * right, pagenum_t right_pagenum);
int insert_into_new_root(page_t * left, int64_t key, page_t * right, pagenum_t right_pagenum);
void start_new_tree(int64_t key, leafRecord leaf_record);
int insert(int64_t key, char* value);

// Deletion.
page_t * remove_entry_from_page(page_t * page, int key_index);
page_t * adjust_root(page_t * root);
page_t * coalesce_nodes(page_t * parent, page_t * key_page, page_t * neighbor, int neighbor_flag, int k_prime);
page_t * redistribute_nodes(page_t * parent_page, page_t * key_page, page_t * neighbor, 
			int neighbor_flag, int k_prime_index, int k_prime);
int get_neighbor_index(page_t * parent, page_t * key_page);
page_t * delete_entry(page_t * key_page, int key_index);
int delete(int64_t key);

// Etc
pagenum_t get_pagenum(page_t* page);
int cut( int length );

#endif /* __BPT_H__*/
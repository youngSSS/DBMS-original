#ifndef __FILE_H__
#define __FILE_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

// Guideline of size
#define PAGE_SIZE 4096
#define PAGE_HEADER_SIZE 128

// Reserved size of each page
#define HEADER_PAGE_RESERVED (PAGE_SIZE - 24)
#define FREE_PAGE_RESERVED (PAGE_SIZE - 8)
#define PAGE_HEADER_RESERVED (PAGE_HEADER_SIZE - 24)

// Number of records in each pages
#define NUM_LEAF_RECORD 31
#define NUM_INTERNAL_RECORD 248

// Value size
#define VALUE_SIZE 120

// Max size of queue
#define MAX 1000

// Default order of leaf page and internal page
#define DEFAULT_LEAF_ORDER 32
#define DEFAULT_INTERNAL_ORDER 249


typedef uint64_t pagenum_t;


/* Structure */

typedef struct internalRecord {
	int64_t key;
	pagenum_t pagenum;
} internalRecord;

typedef struct leafRecord {
	int64_t key;
	char value[VALUE_SIZE];
} leafRecord;

typedef struct headerPage {
	pagenum_t free_pagenum;
	pagenum_t root_pagenum;
	uint64_t num_pages;
	char reserved[HEADER_PAGE_RESERVED];
} headerPage;

typedef struct freePage {
	pagenum_t next_free_pagenum;
	char reserved[FREE_PAGE_RESERVED];
} freePage;

typedef struct page {
	// Page header
	pagenum_t parent_pagenum;
	uint32_t is_leaf;
	uint32_t num_keys;
	char reserved[PAGE_HEADER_RESERVED];
	union {
		// Internal page
		pagenum_t one_more_pagenum;
		// Leaf page
		pagenum_t right_sibling_pagenum;
	};

	union {
		// Internal page : 248 key-pagenum pairs
		internalRecord i_records[NUM_INTERNAL_RECORD];
		// Leaf page : 31 key-value pairs
		leafRecord l_records[NUM_LEAF_RECORD];
	};
} page;

typedef struct page_t {
	union {
		headerPage h;
		freePage f;
		page p;
	};
} page_t;


/* File API functions */

pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_write_page(pagenum_t pagenum, const page_t* src);
int open_file(char * pathname);
int check_file_size(int unique_table_id);

void make_free_pages();

#endif /* __FILE_H__*/
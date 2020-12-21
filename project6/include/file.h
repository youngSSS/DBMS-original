#ifndef __FILE_H__
#define __FILE_H__

#include <sys/types.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <string>

// Guideline of size
#define PAGE_SIZE 4096
#define PAGE_HEADER_SIZE 128

// Reserved size of each page
#define HEADER_PAGE_RESERVED (PAGE_SIZE - 24)
#define FREE_PAGE_RESERVED (PAGE_SIZE - 8)
#define PAGE_HEADER_RESERVED (PAGE_HEADER_SIZE - 40)

// Number of records in each pages
#define NUM_LEAF_RECORD 31
#define NUM_INTERNAL_RECORD 248

// Value size
#define VALUE_SIZE 120

// Default order of leaf page and internal page
#define DEFAULT_LEAF_ORDER 32
#define DEFAULT_INTERNAL_ORDER 249

using namespace std;


typedef uint64_t pagenum_t;


/* Structure */

#pragma pack (push, 1)

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
	char reserved_1[8];
	int64_t page_LSN;
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

#pragma pack(pop)


/* ---------- File APIs ---------- */

// Open & Close

int file_open(char * pathname);
int file_close(int table_id);

// File Modification

pagenum_t file_alloc_page(int table_id);
void file_free_page(int table_id, pagenum_t pagenum);
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest);
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src);

// Help Fuctions

page_t * make_free_pages(int table_id, page_t * header_page);
int get_table_id (string pathname);
int is_open(int table_id);
void file_print_table_list();


#endif /* __FILE_H__*/

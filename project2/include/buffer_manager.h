#ifndef __BUFFER_MANAGER_H__
#define __BUFFER_MANAGER_H__

#include <map>

#include "file.h"

#define NUM_TABLES 11

using namespace std;

typedef int framenum_t;

extern double hit_cnt;
extern double miss_cnt;


/* Buffer Structure */

typedef struct BufferHeader {
	map<pagenum_t, framenum_t> hash_table[NUM_TABLES];
	framenum_t free_framenum;
	int buffer_size;
	framenum_t LRU_head;
	framenum_t LRU_tail;
} BufferHeader;

typedef struct buffer_t {
	page_t frame;
	int table_id;
	pagenum_t pagenum;
	int8_t is_dirty;
	uint32_t pin_cnt;
	framenum_t next_of_LRU;
	framenum_t prev_of_LRU;
} buffer_t;


/* --- For layered architecture --- */

// File

int buf_open(char * pathname);
int buf_close(int table_id);
pagenum_t buf_alloc_page(int table_id);
void buf_free_page(int table_id, pagenum_t pagenum);
void buffer_print_table_list();

/* ---------- Buffer APIs ---------- */

int create_buffer(int num_buf);
int destroy_buffer( void );
void LRU_linking(int framenum);
framenum_t LRU_policy( void );
framenum_t buf_alloc_frame(int table_id, pagenum_t pagenum);
void buf_read_page(int table_id, pagenum_t pagenum, page_t * dest);
void buf_write_page(int table_id, pagenum_t pagenum, const page_t * src);


#endif /* __BUFFER_MANAGER_H__*/
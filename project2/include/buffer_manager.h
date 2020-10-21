#ifndef __BUFFER_MANAGER_H__
#define __BUFFER_MANAGER_H__

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
	int clock_hand;
} BufferHeader;

typedef struct buffer_t {
	page_t frame;
	int table_id;
	pagenum_t pagenum;
	uint32_t is_dirty;
	uint32_t pin_cnt;
	int8_t ref_bit;
	int LRU_list_next;
	framenum_t next_free_framenum;
} buffer_t;


/* --- For layered architecture --- */

// File

int buf_open(char * pathname);
int buf_close(int table_id);
pagenum_t buf_alloc_page(int table_id);
void buf_free_page(int table_id, pagenum_t pagenum);

/* ---------- Buffer APIs ---------- */

int create_buffer(int num_buf);
int destroy_buffer( void );
framenum_t lru_policy(int table_id, pagenum_t pagenum, framenum_t clock_hand);
framenum_t buf_alloc_frame(int table_id, pagenum_t pagenum);
void buf_read_page(int table_id, pagenum_t pagenum, page_t * dest);
void buf_write_page(int table_id, pagenum_t pagenum, const page_t * src);


#endif /* __BUFFER_MANAGER_H__*/
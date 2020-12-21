#ifndef __BUFFER_MANAGER_H__
#define __BUFFER_MANAGER_H__

#include <unordered_map>
#include <pthread.h>

#include "file.h"

#define NUM_TABLES 11

using namespace std;

typedef int framenum_t;

/* Buffer Structure */

typedef struct BufferHeader {
	unordered_map< pagenum_t, framenum_t > hash_table[NUM_TABLES];
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
	pthread_mutex_t page_latch;
	framenum_t next_of_LRU;
	framenum_t prev_of_LRU;
} buffer_t;


/* --- For layered architecture --- */

int buf_open(char * pathname);
int buf_close(int table_id);
pagenum_t buf_alloc_page(int table_id);
void buf_free_page(int table_id, pagenum_t pagenum);
int buf_is_open(int table_id);
void buf_print_table_list();

/* ---------- Buffer APIs ---------- */

// Initializing & Termination
int buf_init_db(int num_buf);
int buf_shutdown_db( void );

// Flush
void buf_flush(int table_id);
void buf_flush_all();

// Replacement
void LRU_linking(int framenum);
framenum_t LRU_policy( void );

// Frame Allocation
framenum_t buf_alloc_frame(int table_id, pagenum_t pagenum);

// Buffer Read & Write
framenum_t get_framenum(int table_id, pagenum_t pagenum, int page_latch_flag);
void buf_read_page(int table_id, pagenum_t pagenum, page_t * dest);
void buf_write_page(int table_id, pagenum_t pagenum, const page_t * src);

// Buffer Read & Write for transaction operations
pthread_mutex_t * mutex_buf_read(int table_id, pagenum_t pagenum, page_t * dest, int page_latch_flag);
void mutex_buf_write(int table_id, pagenum_t pagenum, const page_t * src, int page_latch_flag);


#endif /* __BUFFER_MANAGER_H__*/
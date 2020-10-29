/* Buffer Manager API */

#include "buffer_manager.h"
#include "file.h"


buffer_t * buffer = NULL;
BufferHeader buffer_header;


/* --- For layered architecture --- */

// File

int buf_open(char * pathname) {
	return file_open(pathname);
}


int buf_close(int table_id) {

	// Write all pages of this table from buffer to disk.
	if (buffer != NULL) buf_flush(table_id);

	return file_close(table_id);
}


pagenum_t buf_alloc_page(int table_id) {
	return file_alloc_page(table_id);
}


void buf_free_page(int table_id, pagenum_t pagenum) {
	file_free_page(table_id, pagenum);
}


int buf_is_open(int table_id) {
	return is_open(table_id);
}


void buf_print_table_list() {
	file_print_table_list();
}


/* ---------- Buffer APIs ---------- */

int buf_init_db(int num_buf) {

	if (buffer != NULL) return 2;

	buffer = (buffer_t*)malloc(sizeof(buffer_t) * num_buf);

	if (buffer == NULL) {
		printf("create_buffer fault in buffer_manager.cpp\n");
		return 1;
	}

	// Set buffer
	for (int i = 0;  i < num_buf; i++) {
		buffer[i].table_id = 0;
		buffer[i].pagenum = 0;
		buffer[i].is_dirty = 0;
		buffer[i].pin_cnt = 0;
		buffer[i].next_of_LRU = -1;
		buffer[i].prev_of_LRU = -1;
	}

	// Set buffer header
	buffer_header.free_framenum = 0;
	buffer_header.buffer_size = num_buf;
	buffer_header.LRU_head = -1;
	buffer_header.LRU_tail = -1;

	return 0;
}


int buf_shutdown_db( void ) {
	map<pagenum_t, framenum_t>::iterator iter;

	if (buffer != NULL) {

		for (int i = 1; i <= 10; i++) {
			buf_flush(i);
			file_close(i);
		}

		free(buffer);
		buffer = NULL;
	}

	file_shutdown_db();
	
	return 0;
}


// Write dirty pages of this table from buffer to disk.
void buf_flush(int table_id) {
	map<pagenum_t, framenum_t>::iterator iter;

	for (iter = buffer_header.hash_table[table_id].begin(); iter != buffer_header.hash_table[table_id].end(); iter++) {
		// Write a dirty frame(page) to disk
		if (buffer[iter->second].is_dirty == 1) {
			file_write_page(table_id, iter->first, &buffer[iter->second].frame);
			buffer[iter->second].is_dirty = 0;
		}
	}

	buffer_header.hash_table[table_id].clear();
}


void LRU_linking(int framenum) {

	/* ******************** Mutex Lock ******************** */

	framenum_t old_head, tail;

	// First LRU linking
	if (buffer_header.LRU_head == -1) {
		buffer[framenum].next_of_LRU = framenum;
		buffer[framenum].prev_of_LRU = framenum;

		buffer_header.LRU_head = framenum;
		buffer_header.LRU_tail = framenum;
	}

	else {

		if (buffer_header.LRU_head == framenum)
			return;

		if (buffer[framenum].next_of_LRU != -1) {

			if (buffer_header.LRU_tail == framenum)
				buffer_header.LRU_tail = buffer[framenum].prev_of_LRU;

			buffer[buffer[framenum].next_of_LRU].prev_of_LRU = buffer[framenum].prev_of_LRU;
			buffer[buffer[framenum].prev_of_LRU].next_of_LRU = buffer[framenum].next_of_LRU;
		}

		old_head = buffer_header.LRU_head;
		tail = buffer_header.LRU_tail;

		buffer[framenum].prev_of_LRU = buffer[old_head].prev_of_LRU;
		buffer[framenum].next_of_LRU = buffer[tail].next_of_LRU;

		buffer[old_head].prev_of_LRU = framenum;
		buffer[tail].next_of_LRU = framenum;

		buffer_header.LRU_head = framenum;
	}

	/* ******************* Mutex Unlock ******************* */
}


framenum_t LRU_policy() {
	framenum_t framenum;

	framenum = buffer_header.LRU_tail;

	while (true) {

		if (buffer[framenum].pin_cnt == 0) {

			if (buffer[framenum].is_dirty == 1)
				file_write_page(buffer[framenum].table_id, buffer[framenum].pagenum, &buffer[framenum].frame);

			buffer_header.hash_table[buffer[framenum].table_id].erase(buffer[framenum].pagenum);	

			break;
		}

		else
			framenum = buffer[framenum].prev_of_LRU;
	}

	return framenum;
}


framenum_t buf_alloc_frame(int table_id, pagenum_t pagenum) {
	framenum_t framenum;

	miss_cnt++;

	/* Case : Buffer has no empty frame */

	if (buffer_header.free_framenum == buffer_header.buffer_size)
		framenum = LRU_policy();

	/* Case : Buffer has empty frame */

	else {
		framenum = buffer_header.free_framenum;
		buffer_header.free_framenum++;
	}

	file_read_page(table_id, pagenum, &buffer[framenum].frame);

    buffer[framenum].table_id = table_id;
	buffer[framenum].pagenum = pagenum;
	buffer[framenum].is_dirty = 0;
	buffer[framenum].pin_cnt = 0;

	buffer_header.hash_table[table_id][pagenum] = framenum;

	return framenum;
}


framenum_t get_framenum(int table_id, pagenum_t pagenum) {
	/* *********************************** Mutex Lock *********************************** */
	framenum_t framenum;

	// Buffer dose not have a page
	if (buffer_header.hash_table[table_id].find(pagenum) == buffer_header.hash_table[table_id].end())
		framenum = buf_alloc_frame(table_id, pagenum);

	// Buffer has a page
	else {
		hit_cnt++;
		framenum = buffer_header.hash_table[table_id][pagenum];
	}

	// Set pin
	buffer[framenum].pin_cnt++;

	return framenum;
	/* ********************************** Mutex Unlock ********************************** */
}


void un_pin(int table_id, pagenum_t pagenum, int unpin_cnt) {
	if (buffer == NULL) return;
	
	if (buffer_header.hash_table[table_id].find(pagenum) == buffer_header.hash_table[table_id].end())
		printf("page[%ld] - Target page is not in buffer\nun_pin falut\n", pagenum);
	else {
		buffer[buffer_header.hash_table[table_id][pagenum]].pin_cnt -= unpin_cnt;
		//printf("frame[%ld], page[%d] : %d\n",buffer_header.hash_table[table_id][pagenum], pagenum, buffer[buffer_header.hash_table[table_id][pagenum]].pin_cnt);
	}
}


void buf_read_page(int table_id, pagenum_t pagenum, page_t * dest) {

	framenum_t framenum;

	/* Case : Buffer is not exist */

	if (buffer == NULL){
		file_read_page(table_id, pagenum, dest);
		return;
	}

	/* Case : Buffer is exist */

	framenum = get_framenum(table_id, pagenum);
	
	memcpy(dest, &buffer[framenum].frame, PAGE_SIZE);

	LRU_linking(framenum);
}


void buf_write_page(int table_id, pagenum_t pagenum, const page_t * src) {

	framenum_t framenum;

	/* Case : Buffer is not exist */
	
	if (buffer == NULL) {
		file_write_page(table_id, pagenum, src);
		return;
	}

	/* Case : Buffer is exist */

	framenum = get_framenum(table_id, pagenum);

	memcpy(&buffer[framenum].frame, src, PAGE_SIZE);
	buffer[framenum].is_dirty = 1;

	LRU_linking(framenum);
}

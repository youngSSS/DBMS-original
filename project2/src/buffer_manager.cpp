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
	map<pagenum_t, framenum_t>::iterator iter;

	for (iter = buffer_header.hash_table[table_id].begin(); iter != buffer_header.hash_table[table_id].end(); iter++) {
		// Write a dirty frame(page) to disk
		if (buffer[iter->second].is_dirty == 1)
			file_write_page(table_id, iter->first, &buffer[iter->second].frame);
	}

	buffer_header.hash_table[table_id].clear();

	return file_close(table_id);
}


pagenum_t buf_alloc_page(int table_id) {
	return file_alloc_page(table_id);
}


void buf_free_page(int table_id, pagenum_t pagenum) {
	file_free_page(table_id, pagenum);
}


/* ---------- Buffer APIs ---------- */

int create_buffer(int num_buf) {
	buffer = (buffer_t*)malloc(sizeof(buffer_t) * num_buf);

	if (buffer == NULL) printf("create_buffer fault in buffer_manager.cpp\n");

	// Set buffer
	for (int i = 0;  i < num_buf; i++) {
		buffer[i].table_id = 0;
		buffer[i].pagenum = 0;
		buffer[i].is_dirty = 0;
		buffer[i].pin_cnt = 0;
		buffer[i].ref_bit = 0;
		buffer[i].LRU_list_next = 0;
		buffer[i].next_free_framenum = i + 1;
	}
	buffer[num_buf - 1].next_free_framenum = -1;

	// Set buffer header
	buffer_header.free_framenum = 0;
	buffer_header.buffer_size = num_buf;
	buffer_header.clock_hand = 0;

	printf("Buffer is created\n");

	return 0;
}


int destroy_buffer( void ) {
	map<pagenum_t, framenum_t>::iterator iter;

	if (buffer == NULL) 
		return 1;

	for (int i = 1; i <= 10; i++) {
		for (iter = buffer_header.hash_table[i].begin(); iter != buffer_header.hash_table[i].end(); iter++) {
			// Write a dirty frame(page) to disk
			if (buffer[iter->second].is_dirty == 1)
				file_write_page(i, iter->first, &buffer[iter->second].frame);
		}

		buffer_header.hash_table[i].clear();
	}

	free(buffer);
	buffer = NULL;
	
	return 0;
}


framenum_t lru_policy(int table_id, pagenum_t pagenum, framenum_t clock_hand) {
	framenum_t framenum;

	while (true) {
		if (buffer[clock_hand].pin_cnt == 0 && buffer[clock_hand].ref_bit == 0) {
			
			if (buffer[clock_hand].is_dirty == 1)
				file_write_page(buffer[clock_hand].table_id, buffer[clock_hand].pagenum, &buffer[clock_hand].frame);
		
			buffer_header.hash_table[buffer[clock_hand].table_id].erase(buffer[clock_hand].pagenum);	
			
			file_read_page(table_id, pagenum, &buffer[clock_hand].frame);

			buffer[clock_hand].table_id = table_id;
			buffer[clock_hand].pagenum = pagenum;
			buffer[clock_hand].is_dirty = 0;
			buffer[clock_hand].pin_cnt = 0;
			buffer[clock_hand].ref_bit = 1;
			buffer[clock_hand].LRU_list_next = 0;

			framenum = clock_hand;

			buffer_header.hash_table[table_id][pagenum] = framenum;
			buffer_header.clock_hand = (clock_hand + 1) % buffer_header.buffer_size;

			break;
		}

		else if (buffer[clock_hand].pin_cnt == 0 && buffer[clock_hand].ref_bit == 1)
			buffer[clock_hand].ref_bit = 0;

		clock_hand = (clock_hand + 1) % buffer_header.buffer_size;
	}

	return framenum;
}


framenum_t buf_alloc_frame(int table_id, pagenum_t pagenum) {
	framenum_t framenum;

	framenum = buffer_header.free_framenum;
	buffer_header.free_framenum = buffer[framenum].next_free_framenum;

	file_read_page(table_id, pagenum, &buffer[framenum].frame);

    buffer[framenum].table_id = table_id;
	buffer[framenum].pagenum = pagenum;
	buffer[framenum].is_dirty = 0;
	buffer[framenum].pin_cnt = 0;
	buffer[framenum].ref_bit = 1;
	buffer[framenum].LRU_list_next = 0;

	buffer_header.hash_table[table_id][pagenum] = framenum;

	return framenum;
}


void buf_read_page(int table_id, pagenum_t pagenum, page_t * dest) {

	framenum_t framenum;

	/* Case : Buffer is not exist */

	if (buffer == NULL){
		file_read_page(table_id, pagenum, dest);
		return;
	}

	/* Case : Buffer is exist */

	// Buffer dose not have a page
	if (buffer_header.hash_table[table_id].find(pagenum) == buffer_header.hash_table[table_id].end()) {
		
		/* Case : Buffer has no empty frame */

		if (buffer_header.free_framenum == -1) {
			//printf("********** lru - read\n");
			miss_cnt++;
			framenum = lru_policy(table_id, pagenum, buffer_header.clock_hand);
		}

		/* Case : Buffer has empty frame */

		else {
			//printf("upload to buffer\n");
			miss_cnt++;
			framenum = buf_alloc_frame(table_id, pagenum);
		}
	}

	// Buffer has a page
	else {
		//printf("read from buffer\n");
		hit_cnt++;
		framenum = buffer_header.hash_table[table_id][pagenum];
	}

	// Set pin
	buffer[framenum].pin_cnt++;

	memcpy(dest, &buffer[framenum].frame, PAGE_SIZE);

	// Unset pin
	buffer[framenum].pin_cnt--;
}


void buf_write_page(int table_id, pagenum_t pagenum, const page_t * src) {

	framenum_t framenum;

	/* Case : Buffer is not exist */
	if (buffer == NULL) {
		file_write_page(table_id, pagenum, src);
		return;
	}

	/* Case : Buffer is exist */

	// Buffer dose not have a page
	if (buffer_header.hash_table[table_id].find(pagenum) == buffer_header.hash_table[table_id].end()) {

		/* Case : Buffer has no empty frame */

		if (buffer_header.free_framenum == -1) {
			//printf("\t\t********** LRU - WRITE\n");
			miss_cnt++;
			framenum = lru_policy(table_id, pagenum, buffer_header.clock_hand);
		}

		/* Case : Buffer has empty frame */

		else {
			//printf("\t\tUPLOAD TO BUFFER\n");
			miss_cnt++;
			framenum = buf_alloc_frame(table_id, pagenum);
		}
	}

	// Buffer has a page
	else {
		//printf("\t\tWRITE TO BUUFER\n");
		hit_cnt++;
		framenum = buffer_header.hash_table[table_id][pagenum];
	}

	// Set pin
	buffer[framenum].pin_cnt++;

	memcpy(&buffer[framenum].frame, src, PAGE_SIZE);
	buffer[framenum].is_dirty = 1;

	// Unset pin
	buffer[framenum].pin_cnt--;
}
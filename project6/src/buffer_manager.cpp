/* Buffer Manager API */

#include "buffer_manager.h"
#include "file.h"
#include "log_manager.h"


buffer_t * Buffer = NULL;
BufferHeader Buffer_Header;
pthread_mutex_t Buffer_Latch;


/* --- For layered architecture --- */

// File

int buf_open(char * pathname) {
    return file_open(pathname);
}


int buf_close(int table_id) {

    // Write all pages of this table from buffer to disk.
    if (Buffer != NULL) buf_flush(table_id);

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

    if (Buffer != NULL) return 2;

    Buffer = (buffer_t*)malloc(sizeof(buffer_t) * num_buf);

    if (Buffer == NULL)
        return 1;

    // Set buffer
    for (int i = 0;  i < num_buf; i++) {
        Buffer[i].table_id = 0;
        Buffer[i].pagenum = 0;
        Buffer[i].is_dirty = 0;
        Buffer[i].next_of_LRU = -1;
        Buffer[i].prev_of_LRU = -1;
        if (pthread_mutex_init(&Buffer[i].page_latch, NULL) != 0) return 1;
    }

    // Set buffer header
    Buffer_Header.free_framenum = 0;
    Buffer_Header.buffer_size = num_buf;
    Buffer_Header.LRU_head = -1;
    Buffer_Header.LRU_tail = -1;

    // Set mutex
    if (pthread_mutex_init(&Buffer_Latch, NULL) != 0) return 1;

    return 0;
}


int buf_shutdown_db( void ) {

    if (Buffer != NULL) {

        for (int i = 1; i <= 10; i++) {
            buf_flush(i);
            file_close(i);
        }

        free(Buffer);
        Buffer = NULL;
    }

    return 0;
}


// Write dirty pages of this table from buffer to disk.
void buf_flush(int table_id) {

    unordered_map< pagenum_t, framenum_t >::iterator iter;

    // WAL protocol
    write_log(0, 0);
    write_log(1, 0);

    for (iter = Buffer_Header.hash_table[table_id].begin(); iter != Buffer_Header.hash_table[table_id].end(); iter++) {
        // Write a dirty frame(page) to disk
        if (Buffer[iter->second].is_dirty == 1) {
            file_write_page(table_id, iter->first, &Buffer[iter->second].frame);
            Buffer[iter->second].is_dirty = 0;
        }
    }

    Buffer_Header.hash_table[table_id].clear();
}


void buf_flush_all() {

    for (int i = 1; i <= 10; i++) {
        if (is_open(i) > 0) buf_flush(i);
    }

}


void LRU_linking(int framenum) {

    framenum_t old_head, tail;

    // First LRU linking
    if (Buffer_Header.LRU_head == -1) {
        Buffer[framenum].next_of_LRU = framenum;
        Buffer[framenum].prev_of_LRU = framenum;

        Buffer_Header.LRU_head = framenum;
        Buffer_Header.LRU_tail = framenum;
    }

    else {
        if (Buffer_Header.LRU_head == framenum)
            return;

        if (Buffer[framenum].next_of_LRU != -1) {

            if (Buffer_Header.LRU_tail == framenum)
                Buffer_Header.LRU_tail = Buffer[framenum].prev_of_LRU;

            Buffer[Buffer[framenum].next_of_LRU].prev_of_LRU = Buffer[framenum].prev_of_LRU;
            Buffer[Buffer[framenum].prev_of_LRU].next_of_LRU = Buffer[framenum].next_of_LRU;
        }

        old_head = Buffer_Header.LRU_head;
        tail = Buffer_Header.LRU_tail;

        Buffer[framenum].prev_of_LRU = Buffer[old_head].prev_of_LRU;
        Buffer[framenum].next_of_LRU = Buffer[tail].next_of_LRU;

        Buffer[old_head].prev_of_LRU = framenum;
        Buffer[tail].next_of_LRU = framenum;

        Buffer_Header.LRU_head = framenum;
    }
}


framenum_t LRU_policy() {

    framenum_t framenum;
    framenum = Buffer_Header.LRU_tail;

    while (true) {

        // Success to get a page latch
        if (pthread_mutex_trylock(&Buffer[framenum].page_latch) == 0) {

            if (Buffer[framenum].is_dirty == 1) {
                // WAL protocol
                write_log(0, 0);
                write_log(1, 0);
                file_write_page(Buffer[framenum].table_id, Buffer[framenum].pagenum, &Buffer[framenum].frame);
            }

            Buffer_Header.hash_table[Buffer[framenum].table_id].erase(Buffer[framenum].pagenum);
            break;
        }

        else
            framenum = Buffer[framenum].prev_of_LRU;
    }

    return framenum;
}


framenum_t buf_alloc_frame(int table_id, pagenum_t pagenum) {

    framenum_t framenum;

    /* Case : Buffer has no empty frame */

    if (Buffer_Header.free_framenum == Buffer_Header.buffer_size)
        framenum = LRU_policy();

    /* Case : Buffer has empty frame */

    else {
        framenum = Buffer_Header.free_framenum;
        pthread_mutex_lock(&Buffer[framenum].page_latch);
        Buffer_Header.free_framenum++;
    }

    Buffer_Header.hash_table[table_id][pagenum] = framenum;

    file_read_page(table_id, pagenum, &Buffer[framenum].frame);

    Buffer[framenum].table_id = table_id;
    Buffer[framenum].pagenum = pagenum;
    Buffer[framenum].is_dirty = 0;

    return framenum;
}


framenum_t get_framenum(int table_id, pagenum_t pagenum, int page_latch_flag) {

    framenum_t framenum;

    pthread_mutex_lock(&Buffer_Latch);

    // Buffer dose not have a page
    if (Buffer_Header.hash_table[table_id].find(pagenum) == Buffer_Header.hash_table[table_id].end())
        framenum = buf_alloc_frame(table_id, pagenum);

    // Buffer has a page
    else {
        framenum = Buffer_Header.hash_table[table_id][pagenum];

        if (page_latch_flag == 0) {
            /* Case : Thread fails to catch page latch */
            if (pthread_mutex_trylock(&Buffer[framenum].page_latch) != 0) {
                pthread_mutex_unlock(&Buffer_Latch);
                return -1;
            }
        }
    }

    return framenum;
}


void buf_read_page(int table_id, pagenum_t pagenum, page_t * dest) {

    framenum_t framenum;

    /* Case : Buffer is not exist */
    if (Buffer == NULL){
        file_read_page(table_id, pagenum, dest);
        return;
    }

    /* Case : Buffer is exist */
    framenum = get_framenum(table_id, pagenum, 0);

    /* Case : Thread failed to catch page latch */
    while (framenum == -1)
        framenum = get_framenum(table_id, pagenum, 0);

    LRU_linking(framenum);

    // Unlock buffer latch
    pthread_mutex_unlock(&Buffer_Latch);

    // Read page from buffer
    memcpy(dest, &Buffer[framenum].frame, PAGE_SIZE);

    // Unlock page latch
    pthread_mutex_unlock(&Buffer[framenum].page_latch);

}


void buf_write_page(int table_id, pagenum_t pagenum, const page_t * src) {

    framenum_t framenum;

    /* Case : Buffer is not exist */
    if (Buffer == NULL) {
        file_write_page(table_id, pagenum, src);
        return;
    }

    /* Case : Buffer is exist */
    framenum = get_framenum(table_id, pagenum, 0);

    /* Case : Thread failed to catch page latch */
    while (framenum == -1)
        framenum = get_framenum(table_id, pagenum, 0);

    // Change LRU page
    LRU_linking(framenum);

    // Unlock buffer latch
    pthread_mutex_unlock(&Buffer_Latch);

    // Write page to buffer and set dirty bit
    memcpy(&Buffer[framenum].frame, src, PAGE_SIZE);
    Buffer[framenum].is_dirty = 1;

    // Unlock page latch
    pthread_mutex_unlock(&Buffer[framenum].page_latch);

}


pthread_mutex_t * mutex_buf_read(int table_id, pagenum_t pagenum, page_t * dest, int page_latch_flag) {

    framenum_t framenum;

    /* Case : Buffer is not exist */

    if (Buffer == NULL){
        file_read_page(table_id, pagenum, dest);
        return NULL;
    }

    /* Case : Buffer is exist */

    framenum = get_framenum(table_id, pagenum, page_latch_flag);

    /* Case : Thread failed to catch page latch */
    while (framenum == -1)
        framenum = get_framenum(table_id, pagenum, page_latch_flag);

    // Change LRU page
    LRU_linking(framenum);

    // Unlock buffer latch
    pthread_mutex_unlock(&Buffer_Latch);

    // Read page from buffer
    memcpy(dest, &Buffer[framenum].frame, PAGE_SIZE);

    return &Buffer[framenum].page_latch;
}


void mutex_buf_write(int table_id, pagenum_t pagenum, const page_t * src, int page_latch_flag) {

    framenum_t framenum;

    /* Case : Buffer is not exist */

    if (Buffer == NULL) {
        file_write_page(table_id, pagenum, src);
        return;
    }

    /* Case : Buffer is exist */

    framenum = get_framenum(table_id, pagenum, page_latch_flag);

    /* Case : Thread failed to catch page latch */
    if (framenum == -1)
        printf("mutex_buf_write_ERROR\n");

    LRU_linking(framenum);

    // Unlock buffer latch
    pthread_mutex_unlock(&Buffer_Latch);

    // Write page to buffer and set dirty bit
    memcpy(&Buffer[framenum].frame, src, PAGE_SIZE);
    Buffer[framenum].is_dirty = 1;
}
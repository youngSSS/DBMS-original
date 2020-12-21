/* File Manager API */

#include "file.h"
#include "buffer_manager.h"


// Globals

string Pathname[11] = {"", };
// Is_open has file descriptor value
uint8_t Is_open[11] = {0, };


/* ---------- File APIs ---------- */


int file_open(char * pathname) {
    page_t * header_page;
    string temp_pathname(pathname);
    int file_size;
    int table_id, fd;

    table_id = get_table_id(temp_pathname);

    /* Case : File is already opened */

    if (Is_open[table_id] > 0) 
    	return table_id;

    /* Case : file name format is wrong */

    if (table_id == -2) return -2;

    fd = open(pathname, O_RDWR | O_CREAT, S_IRWXU);

    /* Case : open fail */

    if (fd == -1) return -1;

    /* Case : open success */

    Pathname[table_id] = temp_pathname;
    Is_open[table_id] = fd;

    file_size = lseek(fd, 0, SEEK_END);

    /* Case : file is empty */

    // Create header page
    if (file_size == 0) {
        header_page = (page_t*)malloc(sizeof(page_t));
        if (header_page == NULL) printf("file_open in file.c\n");
        header_page->h.free_pagenum = 0;
        header_page->h.root_pagenum = 0;
        header_page->h.num_pages = 1;

        buf_write_page(table_id, 0, header_page);

        free(header_page);
    }

    return table_id;
}


int file_close(int table_id) {
    int result;

    if (Is_open[table_id] == 0) return 1;
    
    result = close(Is_open[table_id]);

    Is_open[table_id] = 0;

    return result;
}


// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int table_id) { 

    page_t * header_page, * free_page;
    pagenum_t free_pagenum;

    header_page = (page_t*)malloc(sizeof(page_t));
    free_page = (page_t*)malloc(sizeof(page_t));

    if (header_page == NULL) printf("file_alloc_page in file.c\n");
    if (free_page == NULL) printf("file_alloc_page in file.c\n");

    buf_read_page(table_id, 0, header_page);

    free_pagenum = header_page->h.free_pagenum;

    /* No free pages, make 70000 more free pages */
    if (free_pagenum == 0)
        header_page = make_free_pages(table_id, header_page);

    free_pagenum = header_page->h.free_pagenum;

    buf_read_page(table_id, free_pagenum, free_page);

    header_page->h.free_pagenum = free_page->f.next_free_pagenum;

    buf_write_page(table_id, 0, header_page);

    free(header_page);
    free(free_page);

    return free_pagenum;
}


// Free an on-disk page to the free page list
void file_free_page(int table_id, pagenum_t pagenum) { 
    
    page_t * header_page, * target_page;
    pagenum_t old_free_pagenum;
    off_t offset = PAGE_SIZE * pagenum;

    header_page = (page_t*)malloc(sizeof(page_t));
    target_page = (page_t*)malloc(sizeof(page_t));

    if (header_page == NULL) printf("file_free_page in file.c\n");
    if (target_page == NULL) printf("file_free_page in file.c\n");

    buf_read_page(table_id, 0, header_page);
    buf_read_page(table_id, pagenum, target_page);

    old_free_pagenum = header_page->h.free_pagenum;
    header_page->h.free_pagenum = pagenum;
    target_page->f.next_free_pagenum = old_free_pagenum;

    buf_write_page(table_id, 0, header_page);
    buf_write_page(table_id, pagenum, target_page);

    free(header_page);
    free(target_page);
}


// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int table_id, pagenum_t pagenum, page_t * dest) { 
    int result, fd;

    fd = Is_open[table_id];

    result = pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);

    if (result == -1)
        printf("read_from_file fault in file.c\n");
}


// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t * src) { 
    int result, fd;

    fd = Is_open[table_id];

    result = pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);

    if (fsync(fd) != 0) printf("fsync fault\n");

    if (result == -1) printf("write_to_file fault in file.c\n");
}


// Make 10000 free pages
page_t * make_free_pages(int table_id, page_t * header_page) {
    
    page_t * free_page;
    pagenum_t start_free_pagenum;
    int i;

    free_page = (page_t*)malloc(sizeof(page_t));
    if (free_page == NULL) printf("make_free_pages in file.cpp\n");

    // Make a free page and write it on on-disk
    start_free_pagenum = header_page->h.num_pages;
    for (i = 0; i < 100; i++) {
        free_page->f.next_free_pagenum = start_free_pagenum + i + 1;
        file_write_page(table_id, start_free_pagenum + i, free_page);
        header_page->h.num_pages++;
    }

    free_page->f.next_free_pagenum = 0;
    header_page->h.num_pages++;

    header_page->h.free_pagenum = start_free_pagenum;

    file_write_page(table_id, start_free_pagenum + i, free_page);

    free(free_page);

    return header_page;
}


int get_table_id (string pathname) {

	for (int i = 1; i <= 10; i++) {
		if (pathname == "DATA" + to_string(i))
			return i;
	}
	return -2;
}


int is_open(int table_id) {
    return Is_open[table_id];
}


void file_print_table_list() {
	for (int i = 1; i <= 10; i++) {
		if (Is_open[i] > 0)
			printf("Table id : [ %d ], File name : [ %s ]\n", i, Pathname[i].c_str());
		else
			printf("Table id : [ - ], File name : [ %s ]  ** Not opened **\n", Pathname[i].c_str());
	}
}

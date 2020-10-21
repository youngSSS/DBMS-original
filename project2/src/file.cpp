/* File Manager API */

#include "file.h"
#include "buffer_manager.h"


// Globals
map<int, string> Table_id_pathname;
map<string, int> Pathname_table_id;
map<int, int> Table_id_fd;
int Table_id_list[11] = {0, };


/* ---------- File APIs ---------- */


int file_open(char * pathname) {
    page_t * header_page;
    string temp_pathname(pathname);
    int file_size;
    int table_id, fd;
    
    /* Case : pathname file is already open */

    if (Pathname_table_id.find(temp_pathname) != Pathname_table_id.end()) {
        printf("File is already open\nTable_id : %d\n", Pathname_table_id[temp_pathname]);
        return Pathname_table_id[temp_pathname];
    }

    fd = open(pathname, O_RDWR | O_CREAT, S_IRWXU);

    /* Case : open fail */

    if (fd == -1) return -1;

    table_id = file_alloc_table_id(fd);

    /* Case : open more than 10 pages */

    if (table_id == -1) return -2;

    /* Case : open success */

    Table_id_pathname[table_id] = temp_pathname;
    Pathname_table_id[temp_pathname] = table_id;

    file_size = lseek(table_id, 0, SEEK_END);

    /* Case : file is empty */

    // Create header page
    if (file_size == 0) {
        header_page = (page_t*)malloc(sizeof(page_t));
        header_page->h.free_pagenum = 0;
        header_page->h.root_pagenum = 0;
        header_page->h.num_pages = 1;

        buf_write_page(table_id, 0, header_page);
        free(header_page);
    }

    return table_id;
}


int file_close(int table_id) {
    int fd;
    
    /* Case : file having table_id is not exist */

    if (Table_id_pathname.find(table_id) == Table_id_pathname.end())
        return 1;

    /* Case : close */

    fd = Table_id_fd[table_id];

    Pathname_table_id.erase(Table_id_pathname[table_id]);
    Table_id_pathname.erase(table_id);
    Table_id_list[table_id] = 0;

    return close(fd);
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

    fd = Table_id_fd[table_id];

    result = pread(fd, dest, PAGE_SIZE, pagenum * PAGE_SIZE);

    if (result == -1) printf("read_from_file fault in disk_manager.c\n");
}


// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t * src) { 
    int result, fd;

    fd = Table_id_fd[table_id];
    
    result = pwrite(fd, src, PAGE_SIZE, pagenum * PAGE_SIZE);

    if (fsync(table_id) != 0) printf("fsync fault\n");

    if (result == -1) printf("write_to_file fault in disk_manager.c\n");
}


int file_alloc_table_id(int fd) {
    int table_id, i;

    for (i = 1; i <= 10; i++) {
        if (Table_id_list[i] == 0) {
            Table_id_list[i] = 1;
            table_id = i;
            break;
        }
    }

    if (i == 11) return -1;

    Table_id_fd[table_id] = fd;

    return table_id;
}


// Make 10000 free pages
page_t * make_free_pages(int table_id, page_t * header_page) {
    
    page_t * free_page;
    pagenum_t start_free_pagenum;
    int i;

    free_page = (page_t*)malloc(sizeof(page_t));
    if (free_page == NULL) printf("make_free_pages in file.cpp\n");

    printf("Start to make 10000 free pages\n");

    // Make a free page and write it on on-disk
    start_free_pagenum = header_page->h.num_pages;
    for (i = 0; i < 10000; i++) {
        free_page->f.next_free_pagenum = start_free_pagenum + i + 1;
        file_write_page(table_id, start_free_pagenum + i, free_page);
        header_page->h.num_pages++;
    }

    free_page->f.next_free_pagenum = 0;
    header_page->h.num_pages++;

    header_page->h.free_pagenum = start_free_pagenum;

    file_write_page(table_id, start_free_pagenum + i, free_page);

    free(free_page);

	printf("End\n");

    return header_page;
}

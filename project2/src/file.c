/* File Manager API */

#include "file.h"
#include "disk_manager.h"


// Global variable
page_t * header_page;


// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(){ 
    pagenum_t free_pagenum;
    page_t* free_page = (page_t*)malloc(sizeof(page_t));

    free_pagenum = header_page->h.free_pagenum;

    /* No free pages, make 70000 more free pages */
    if (free_pagenum == 0)
        make_free_pages();

    free_pagenum = header_page->h.free_pagenum;

    file_read_page(free_pagenum, free_page);
    header_page->h.free_pagenum = free_page->f.next_free_pagenum;
    file_write_page(0, header_page);

    return free_pagenum;
}


// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum){ 
    off_t offset = PAGE_SIZE * pagenum;
    page_t* target_page = (page_t*)malloc(sizeof(page_t));
    pagenum_t old_free_pagenum;

    if (target_page == NULL) printf("file_free_page in file.c\n");

    file_read_page(pagenum, target_page);

    /* Change a header page's free_page_num to target page number
     * Link free pages by making target page's next_free_pagenum to
     * header page's old free_page_num
     */
    old_free_pagenum = header_page->h.free_pagenum;
    header_page->h.free_pagenum = pagenum;
    target_page->f.next_free_pagenum = old_free_pagenum;

    file_write_page(0, header_page);
    file_write_page(pagenum, target_page);

    free(target_page);
}


// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest){ 
    read_from_disk(Unique_table_id, dest, PAGE_SIZE, PAGE_SIZE * pagenum);
}


// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src){ 
    write_to_disk(Unique_table_id, src, PAGE_SIZE, PAGE_SIZE * pagenum);
}


int open_file(char * pathname) {
    return open_file_from_disk(pathname);
}


int check_file_size(int unique_table_id){
    return check_file_size_from_disk(unique_table_id);
}



/* When there is no free page for file,
 * make 70000 free pages (because, 3 floor b+ tree has 61504 pages)
 */
void make_free_pages() {
    int i;
    pagenum_t start_free_pagenum;
    page_t* free_page = (page_t*)malloc(sizeof(page_t));
    if (free_page == NULL) printf("make_header_page in db_api.c\n");
	
	printf("Start to make 70000 free pages\n");

    // Make a free page and write it on on-disk
    start_free_pagenum = header_page->h.num_pages;
    for (i = 0; i < 70000; i++) {
        free_page->f.next_free_pagenum = i + start_free_pagenum + 1;
        file_write_page(i + start_free_pagenum, free_page);
        header_page->h.num_pages++;
    }

    free_page->f.next_free_pagenum = 0;
    file_write_page(i + start_free_pagenum, free_page);
    header_page->h.num_pages++;

    free(free_page);

    /* Write header page to on-disk
     * because, header page's free_pagenum is changed to 1 from 0
     */
    header_page->h.free_pagenum = start_free_pagenum;
    file_write_page(0, header_page);

	printf("End\n");
}

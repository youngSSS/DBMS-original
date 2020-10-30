/* Disk Manager API */

#include "file.h"
#include "disk_manager.h"


void read_from_disk(int table_id, page_t* dest, int page_size, off_t offset) {
    int result;

    result = pread(table_id, dest, PAGE_SIZE, offset);

    if (result == -1) printf("read_from_file fault in disk_manager.c\n");
}


void write_to_disk(int table_id, const page_t* src, int page_size, off_t offset) {
    int result;
    
	result = pwrite(table_id, src, PAGE_SIZE, offset);
	if (fsync(table_id) != 0) printf("fsync fault\n");

    if (result == -1) printf("write_to_file fault in disk_manager.c\n");
}


int open_file_from_disk(char * pathname) {
	return open(pathname, O_RDWR | O_CREAT, S_IRWXU);
}


int close_file_from_disk(int table_id) {
	return close(table_id);
}


int check_file_size_from_disk(int table_id) {
	return lseek(table_id, 0, SEEK_END);
}
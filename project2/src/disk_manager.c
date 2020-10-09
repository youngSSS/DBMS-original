/* Disk Manager API */

#include "file.h"
#include "disk_manager.h"


int open_file_from_disk(char * pathname) {
	return open(pathname, O_RDWR | O_CREAT | O_SYNC | O_DIRECT, S_IRWXU);
}


int check_file_size_from_disk(int unique_table_id) {
	return lseek(unique_table_id, 0, SEEK_END);
}


void read_from_disk(int table_id, page_t* dest, int page_size, off_t offset) {
    int result;

    result = pread(Unique_table_id, dest, PAGE_SIZE, offset);

    if (result == -1) printf("read_from_file fault in disk_manager.c\n");
}


void write_to_disk(int table_id, const page_t* src, int page_size, off_t offset) {
    int result;

    result = pwrite(Unique_table_id, src, PAGE_SIZE, offset);

    if (result == -1) printf("write_to_file fault in disk_manager.c\n");
}

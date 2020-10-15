#ifndef __DISK_MANAGER_H__
#define __DISK_MANAGER_H__

#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

/* Disk access functions */

void read_from_disk(int table_id, page_t* dest, int page_size, off_t offset);
void write_to_disk(int table_id, const page_t* src, int page_size, off_t offset);
int open_file_from_disk(char* pathname);
int close_file_from_disk(int table_id);
int check_file_size_from_disk(int table_id);

#endif /* __DISK_MANAGER_H__*/
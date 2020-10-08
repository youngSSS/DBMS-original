#ifndef __DISK_MANAGER_H__
#define __DISK_MANAGER_H__

#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

extern int Unique_table_id;

/* Disk access functions */

int open_file_from_disk(char* pathname);
int check_file_size_from_disk(int unique_table_id);
void read_from_disk(int table_id, page_t* dest, int page_size, off_t offset);
void write_to_disk(int table_id, const page_t* src, int page_size, off_t offset);

#endif /* __DISK_MANAGER_H__*/
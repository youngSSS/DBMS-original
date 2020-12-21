#ifndef __LOG_MANAGER_H__
#define __LOG_MANAGER_H__

#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdint.h>
#include <vector>
#include <unordered_map>
#include <unistd.h>
#include <fcntl.h>
#include <cstdio>

#define LOG_BUFFER_SIZE     10240000

#define LOG_G_SIZE          28
#define LOG_U_SIZE          288
#define LOG_C_SIZE          296

#define DATA_LENGTH         120

#define TYPE_START_POS      24
#define TYPE_SIZE           4

#define IMG_SIZE            120

#define TRX_ID_START_POS    20
#define TRX_ID_SIZE         4

#define TEMP_LOG_BUFFER_SIZE 15000

#define LSN_FILE "LSN_FILE"


using namespace std;

#pragma pack (push, 1)

// Log type for BEGIN(0), COMMIT(2), ROLLBACK(3)
typedef struct logG_t {
    int32_t log_size;
    int64_t lsn;
    int64_t prev_lsn;
    int32_t trx_id;
    int32_t type;
} logG_t;

// Log type for UPDATE(1)
typedef struct logU_t {
    int32_t log_size;
    int64_t lsn;
    int64_t prev_lsn;
    int32_t trx_id;
    int32_t type;
    int32_t table_id;
    int64_t pagenum;
    int32_t offset;
    int32_t data_length;
    char old_img[120];
    char new_img[120];
} logU_t;

// Log type for COMPENSATE(4)
typedef struct logC_t {
    int32_t log_size;
    int64_t lsn;
    int64_t prev_lsn;
    int32_t trx_id;
    int32_t type;
    int32_t table_id;
    int64_t pagenum;
    int32_t offset;
    int32_t data_length;
    char old_img[120];
    char new_img[120];
    int64_t next_undo_LSN;
} logC_t;


#pragma pack(pop)


/* --------------- Functions for logging --------------- */

// Initiate log manager
int init_log(char * logmsg_path);

// Issue a log
int64_t issue_begin_log (int trx_id);
int64_t issue_commit_log (int trx_id);
int64_t issue_rollback_log (int trx_id);
int64_t issue_update_log (int32_t trx_id, int32_t table_id, int64_t pagenum, int32_t offset, char * old_img, char * new_img);
int64_t issue_compensate_log (int32_t trx_id, int32_t table_id, int64_t pagenum, int32_t offset, char * old_img, char * new_img, int64_t next_undo_LSN);

// Issue a LSN
int64_t issue_LSN(int log_size);
int64_t get_and_update_last_LSN(int trx_id, int64_t lsn);

// Write log buffer to log disk
void write_log(int temp_flag, int no_latch_flag);
// Write log to log buffer
void write_to_log_buffer(void * log, int type);


/* --------------- Functions for recovery --------------- */
int DB_recovery(int flag, int log_num, char * log_path);
int analysis_pass(int read_size, int * start_offset);
int redo_pass(int flag, int log_num, int read_size, int * start_offset, unordered_map< int, int > loser);
int undo_pass(int flag, int log_num, int * start_offset);

// Print log buffer
void print_log();


#endif /* __LOG_MANAGER_H__ */
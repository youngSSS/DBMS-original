#include "log_manager.h"
#include "buffer_manager.h"
#include "file.h"

// ERASE!! this header just erase do not wonder
#include "index_manager.h"


// Log Buffer
char Log_Buffer[LOG_BUFFER_SIZE];
int Log_Buf_Offset = 0;

// Temp Log Buffer (This is used for CLR issued during recovery)
char Temp_Log_Buffer[TEMP_LOG_BUFFER_SIZE];
int Temp_Log_Buf_Offset = 0;

// LSN
int64_t Global_LSN = 0;

// Trace the last LSN of each transaction
unordered_map< int, int64_t> Trx_Last_LSN;

// Mutex for Log_Buffer, Log_Offset, Clr_Log_Buffer, Clr_Log_Buf_Offset
pthread_mutex_t Log_Buffer_Latch;
pthread_mutex_t Log_Write_Latch;

// Mutex for Global_LSN
pthread_mutex_t LSN_Latch;
// Mutex for Trx_Last_LSN
pthread_mutex_t Trx_Last_LSN_Latch;

// Log File file descriptor
int Log_File_Fd = 0;
int LSN_FD = 0;


/* RECOVERY */

// Used in analysis pass
unordered_map< int, int > Winner_Check;

// Undo
unordered_map< int, vector<int> > Undo_Map; // trx_id, log disk offset vector
vector< pair<int, int>  > Undo_List; // trx_id, log disk offset

// The start offset of undo logs
vector< int > Start_Offset;

// Next undo LSN
unordered_map< int32_t, int64_t > Next_Undo_LSN;

// Log message file
FILE * Log_FP;

// Crash
int Log_Num = 0;


int init_log(char * logmsg_path) {

    // Initiate Log Buffer latch
    if (pthread_mutex_init(&Log_Buffer_Latch, NULL) != 0) return 1;

    // Initiate Log Buffer latch
    if (pthread_mutex_init(&Log_Write_Latch, NULL) != 0) return 1;

    // Initiate LSN latch
    if (pthread_mutex_init(&LSN_Latch, NULL) != 0) return 1;

    // Initiate Trx LSN latch
    if (pthread_mutex_init(&Trx_Last_LSN_Latch, NULL) != 0) return 1;

    // Keep LSN
    if (access(LSN_FILE, F_OK) != 0) {
        LSN_FD = open(LSN_FILE, O_RDWR | O_CREAT, S_IRWXU);
        pwrite(LSN_FD, &Global_LSN, 8, 0);
    }
    else {
        LSN_FD = open(LSN_FILE, O_RDWR | O_CREAT, S_IRWXU);
        pread(LSN_FD, &Global_LSN, 8, 0);
    }

    remove(logmsg_path);
    Log_FP = fopen(logmsg_path, "w");

    return 0;
}


int64_t issue_begin_log (int trx_id) {

    logG_t log;

    log.log_size = LOG_G_SIZE;
    log.lsn = issue_LSN(LOG_G_SIZE);
    log.prev_lsn = get_and_update_last_LSN(trx_id, log.lsn);
    log.trx_id = trx_id;
    log.type = 0;

    write_to_log_buffer(&log, 0);

    return log.lsn;
}


int64_t issue_commit_log (int trx_id) {

    logG_t log;

    log.log_size = LOG_G_SIZE;
    log.lsn = issue_LSN(LOG_G_SIZE);
    log.prev_lsn = get_and_update_last_LSN(trx_id, log.lsn);
    log.trx_id = trx_id;
    log.type = 2;

    write_to_log_buffer(&log, 2);

    return log.lsn;
}


int64_t issue_rollback_log (int trx_id) {

    logG_t log;

    log.log_size = LOG_G_SIZE;
    log.lsn = issue_LSN(LOG_G_SIZE);
    log.prev_lsn = get_and_update_last_LSN(trx_id, log.lsn);
    log.trx_id = trx_id;
    log.type = 3;

    write_to_log_buffer(&log, 3);

    return log.lsn;
}


int64_t issue_update_log (int32_t trx_id, int32_t table_id, int64_t pagenum, int32_t offset,
                          char * old_img, char * new_img) {

    logU_t log;

    log.log_size = LOG_U_SIZE;
    log.lsn = issue_LSN(LOG_U_SIZE);
    log.prev_lsn = get_and_update_last_LSN(trx_id, log.lsn);
    log.trx_id = trx_id;
    log.type = 1;
    log.table_id = table_id;
    log.pagenum = pagenum;
    log.offset = offset;
    log.data_length = DATA_LENGTH;
    strcpy(log.old_img, old_img);
    strcpy(log.new_img, new_img);

    write_to_log_buffer(&log, 1);

    return log.lsn;
}


int64_t issue_compensate_log (int32_t trx_id, int32_t table_id, int64_t pagenum, int32_t offset,
                              char * old_img, char * new_img, int64_t next_undo_LSN) {

    logC_t log;

    log.log_size = LOG_C_SIZE;
    log.lsn = issue_LSN(LOG_C_SIZE);
    log.prev_lsn = get_and_update_last_LSN(trx_id, log.lsn);
    log.trx_id = trx_id;
    log.type = 4;
    log.table_id = table_id;
    log.pagenum = pagenum;
    log.offset = offset;
    log.data_length = DATA_LENGTH;
    strcpy(log.old_img, new_img);
    strcpy(log.new_img, old_img);
    log.next_undo_LSN = next_undo_LSN;

    write_to_log_buffer(&log, 4);

    return log.lsn;
}


// Serves a LSN
int64_t issue_LSN(int log_record_size) {
    int log_sequence_number;

    /* LSN is equal to log's offset in log disk
     * REAL OFFSET !!!!!!!!!
     * */

    pthread_mutex_lock(&LSN_Latch);
    log_sequence_number = Global_LSN;
    Global_LSN = Global_LSN + log_record_size;
    pthread_mutex_unlock(&LSN_Latch);

    return log_sequence_number;
}


// Last LSN getter of each transaction, after getting update it
int64_t get_and_update_last_LSN(int trx_id, int64_t lsn) {
    int last_lsn;

    pthread_mutex_lock(&Trx_Last_LSN_Latch);

    /* Case : first log of trx_id transaction */
    if (Trx_Last_LSN.count(trx_id) == 0) last_lsn = -1;
    else last_lsn = Trx_Last_LSN[trx_id];

    Trx_Last_LSN[trx_id] = lsn;

    pthread_mutex_unlock(&Trx_Last_LSN_Latch);

    return last_lsn;
}


// Write log buffer to disk
void write_log(int temp_flag, int no_latch_flag) {

    if (no_latch_flag == 0)
        pthread_mutex_lock(&Log_Buffer_Latch);

    int start_offset;
    start_offset = lseek(Log_File_Fd, 0, SEEK_END);

    if (temp_flag == 0) {
        if (Log_Buf_Offset != 0) {

            if (pwrite(Log_File_Fd, Log_Buffer, Log_Buf_Offset, start_offset) == -1)
                printf("log_write fault in log_manager.cpp");
            if (pwrite(LSN_FD, &Global_LSN, 8, 0) == -1)
                printf("LSN write fault in log_manager.cpp");

            Log_Buf_Offset = 0;

        }
    }

    else {
        if (Temp_Log_Buf_Offset != 0) {

            if (pwrite(Log_File_Fd, Temp_Log_Buffer, Temp_Log_Buf_Offset, start_offset) == -1)
                printf("clr_log_write fault in log_manager.cpp");
            if (pwrite(LSN_FD, &Global_LSN, 8, 0) == -1)
                printf("LSN write fault in log_manager.cpp");

            Temp_Log_Buf_Offset = 0;

        }
    }

    start_offset = lseek(Log_File_Fd, 0, SEEK_END);

    if (no_latch_flag == 0)
        pthread_mutex_unlock(&Log_Buffer_Latch);

}


// Write log to log buffer
void write_to_log_buffer(void * log, int type) {

    logG_t temp_log;

    pthread_mutex_lock(&Log_Buffer_Latch);

    if (type == 1) {

        // If Log Buffer is full, flush
        // Push log to log buffer
        if (Log_Buf_Offset + LOG_U_SIZE > LOG_BUFFER_SIZE)
            write_log(0, 1);

        memcpy(&Log_Buffer[Log_Buf_Offset], (logU_t*)log, LOG_U_SIZE);
        Log_Buf_Offset += LOG_U_SIZE;

    }

    else if (type == 4) {

        // If Log Buffer is full, flush
        // Push log to log buffer
        if (Log_Buf_Offset + LOG_C_SIZE > LOG_BUFFER_SIZE)
            write_log(0, 1);

        memcpy(&Log_Buffer[Log_Buf_Offset], (logC_t*)log, LOG_C_SIZE);
        Log_Buf_Offset += LOG_C_SIZE;

    }

    else {

        // If Log Buffer is full, flush
        // Push log to log buffer
        if (Log_Buf_Offset + LOG_G_SIZE > LOG_BUFFER_SIZE)
            write_log(0, 1);

        memcpy(&Log_Buffer[Log_Buf_Offset], (logG_t*)log, LOG_G_SIZE);
        Log_Buf_Offset += LOG_G_SIZE;

        memcpy(&temp_log, &Log_Buffer[Log_Buf_Offset - LOG_G_SIZE], LOG_G_SIZE);

    }

    pthread_mutex_unlock(&Log_Buffer_Latch);

}


int DB_recovery(int flag, int log_num, char * log_path) {

    unordered_map< int, int >           loser;
    unordered_map< int, int >::iterator iter;

    int log_file_size;
    int temp_log_file_size;
    int read_size;
    int * offset;
    int undo_offset;
    int result;


    /* Case : Nothing to recover
     * If log file is not exist, make a log file and return 0
     */
    if (access(log_path, F_OK) != 0) {
        Log_File_Fd = open(log_path, O_RDWR | O_CREAT, S_IRWXU);
        fclose(Log_FP);
        return 0;
    }

    /* Case : Something to recover
     * If log file is not exist, make a log file and return 0
     */

    // Open Log File
    Log_File_Fd = open(log_path, O_RDWR | O_CREAT, S_IRWXU);
    log_file_size = lseek(Log_File_Fd, 0, SEEK_END);

    if (log_file_size == 0) return 0;

    offset = (int*)malloc(sizeof(int));
    *offset = 0;

    /* Case : Log Buffer is equal or larger than Log File */
    if (log_file_size <= LOG_BUFFER_SIZE) {

        read_size = log_file_size;

        result = pread(Log_File_Fd, Log_Buffer, read_size, *offset);
        if (result == -1) printf("Read Error in DB_recovery\n");

        /* -------------------- Analysis Pass -------------------- */
        fprintf(Log_FP, "[ANALYSIS] Analysis pass start\n");

        analysis_pass(read_size, offset);

        fprintf(Log_FP, "[ANALYSIS] Analysis success. ");

        fprintf(Log_FP, "Winner: ");
        for (iter = Winner_Check.begin(); iter != Winner_Check.end(); iter++) {
            if (iter->second == 0) loser[iter->first] = 1;
            else fprintf(Log_FP, "%d ", iter->first);
        }
        fprintf(Log_FP, ",Loser: ");
        for (iter = loser.begin(); iter != loser.end(); iter++)
            fprintf(Log_FP, "%d ", iter->first);
        fprintf(Log_FP, "\n");

        /* ------------------------------------------------------- */


        /* ---------------------- Redo Pass ---------------------- */
        fprintf(Log_FP, "[REDO] Redo pass start\n");

        *offset = 0;
        result = redo_pass(flag, log_num, read_size, offset, loser);
        if (result == 1) return 0;

        // I want to crash in redo, but log_num is too big
        if (flag == 1) {
            buf_flush_all();
            fclose(Log_FP);
            Undo_Map.clear();
            Undo_List.clear();
            Winner_Check.clear();
            Next_Undo_LSN.clear();
            free(offset);
            return 0;
        }

        fprintf(Log_FP, "[REDO] Redo pass end\n");
        /* ------------------------------------------------------- */


        /* ---------------------- Undo Pass ---------------------- */
        fprintf(Log_FP, "[UNDO] Undo pass start\n");

        Log_Num = 0;
        *offset = 0;
        result = undo_pass(flag, log_num, offset);
        if (result == 1) return 0;

        fprintf(Log_FP, "[UNDO] Undo pass end\n");
        /* ------------------------------------------------------- */


    }

    /* Case : Log Buffer is smaller than Log File */
    else {

        /* -------------------- Analysis Pass -------------------- */
        fprintf(Log_FP, "[ANALYSIS] Analysis pass start\n");

        temp_log_file_size = log_file_size;
        *offset = 0;
        while (true) {

            if ( (temp_log_file_size / LOG_BUFFER_SIZE) >= 1 )
                read_size = LOG_BUFFER_SIZE;
            else
                read_size = temp_log_file_size;

            result = pread(Log_File_Fd, Log_Buffer, read_size, *offset);
            if (result == -1) printf("Read Error in DB_recovery\n");

            analysis_pass(read_size, offset);

            temp_log_file_size = log_file_size - *offset;
            if (temp_log_file_size <= 0) break;

        }

        fprintf(Log_FP, "[ANALYSIS] Analysis success. ");

        fprintf(Log_FP, "Winner: ");
        for (iter = Winner_Check.begin(); iter != Winner_Check.end(); iter++) {
            if (iter->second == 0) loser[iter->first] = 1;
            else fprintf(Log_FP, "%d ", iter->first);
        }
        fprintf(Log_FP, ",Loser: ");
        for (iter = loser.begin(); iter != loser.end(); iter++)
            fprintf(Log_FP, "%d ", iter->first);
        fprintf(Log_FP, "\n");
        /* ------------------------------------------------------- */


        /* ---------------------- Redo Pass ---------------------- */
        fprintf(Log_FP, "[REDO] Redo pass start\n");

        temp_log_file_size = log_file_size;
        *offset = 0;
        while (true) {

            Start_Offset.push_back(*offset);

            if ( (temp_log_file_size / LOG_BUFFER_SIZE) >= 1 )
                read_size = LOG_BUFFER_SIZE;
            else
                read_size = temp_log_file_size;

            result = pread(Log_File_Fd, Log_Buffer, read_size, *offset);
            if (result == -1) printf("Read Error in DB_recovery\n");

            result = redo_pass(flag, log_num, read_size, offset, loser);
            if (result == 1) return 0;

            temp_log_file_size = log_file_size - *offset;
            if (temp_log_file_size <= 0) break;

        }

        // I want to crash in redo, but log_num is too big
        if (flag == 1) {
            buf_flush_all();
            fclose(Log_FP);
            Undo_Map.clear();
            Undo_List.clear();
            Winner_Check.clear();
            Next_Undo_LSN.clear();
            return 0;
        }

        fprintf(Log_FP, "[REDO] Redo pass end\n");
        /* ------------------------------------------------------- */


        /* ---------------------- Undo Pass ---------------------- */
        fprintf(Log_FP, "[UNDO] Undo pass start\n");

        Log_Num = 0;
        while (true) {

            if (Undo_List.size() == 0)
                break;

            else {

                *offset = Start_Offset.back();
                Start_Offset.pop_back();

                undo_offset = Undo_List.back().second;

                while (undo_offset < *offset) {
                    *offset = Start_Offset.back();
                    Start_Offset.pop_back();
                }

            }

            result = pread(Log_File_Fd, Log_Buffer, LOG_BUFFER_SIZE, *offset);
            if (result == -1) printf("Read Error in DB_recovery\n");

            result = undo_pass(flag, log_num, offset);
            if (result == 1) return 0;

        }

        fprintf(Log_FP, "[UNDO] Undo pass end\n");
        /* ------------------------------------------------------- */

    }

    // WAL protocol
    write_log(1, 0);

    // Flush data buffer
    // Apply recovered data to disk - Atomicity & Durability
    buf_flush_all();

    // Delete LogFile.db
    // After successeful recovery, Log file is not useful
    remove(log_path);

    // Open Log File
    // This log file is used for after recovery operations
    Log_File_Fd = open(log_path, O_RDWR | O_CREAT, S_IRWXU);

    // Close log message file pointer
    fclose(Log_FP);

    Undo_Map.clear();
    Undo_List.clear();
    Winner_Check.clear();
    Next_Undo_LSN.clear();

    free(offset);

    return 0;
}


int analysis_pass(int read_size, int * start_offset) {

    unordered_map< int, int >::iterator iter;
    int32_t trx_id;
    int32_t type;
    int offset;

    offset = 0;

    while (true) {

        memcpy(&type, &Log_Buffer[offset + TYPE_START_POS], TYPE_SIZE);

        /* Case : UPDATE Log, forward offset 284 */
        if (type == 1) offset += LOG_U_SIZE;

        /* Case : Compensate Log, forward offset 292 */
        else if (type == 4) offset += LOG_C_SIZE;

        /* Case : BEGIN, COMMIT, ROLLBACK Log, forward offset 28 */
        else {

            memcpy(&trx_id, &Log_Buffer[offset + TRX_ID_START_POS], TRX_ID_SIZE);

            // BEGIN Log
            if (type == 0) {
                if (Winner_Check.count(trx_id) != 0)
                    printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                           "@@@@@@@@@ ERROR 1 - 3 @@@@@@@@\n"
                           "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    );
                Winner_Check[trx_id] = 0;
            }

            // COMMIT Log
            else if (type == 2) {
                if (Winner_Check.count(trx_id) == 0)
                    printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                           "@@@@@@@@@ ERROR 1 - 1 @@@@@@@@\n"
                           "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    );
                Winner_Check[trx_id] = 1;
            }

            // ROLLBACK Log
            else {
                if (Winner_Check.count(trx_id) == 0)
                    printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                           "@@@@@@@@@ ERROR 1 - 2 @@@@@@@@\n"
                           "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                    );
                Winner_Check[trx_id] = 1;
            }

            offset += LOG_G_SIZE;

        }

        /* Loop termination condition */
        memcpy(&type, &Log_Buffer[offset + TYPE_START_POS], TYPE_SIZE);

        if (read_size - offset == 0) {
            *start_offset += offset;
            break;
        }

        else {

            if (type == 1) {
                if (read_size - offset < LOG_U_SIZE) {
                    *start_offset += offset;
                    break;
                }
            }

            else if (type == 4) {
                if (read_size - offset < LOG_C_SIZE) {
                    *start_offset += offset;
                    break;
                }
            }

            else {
                if (read_size - offset < LOG_G_SIZE) {
                    *start_offset += offset;
                    break;
                }
            }

        }

    }

    return 0;

}


int redo_pass(int flag, int log_num, int read_size, int * start_offset, unordered_map< int, int > loser) {

    int32_t type;
    int offset;
    int record_index;

    page_t * page;
    logG_t g_log;
    logU_t update_log;
    logC_t compensate_log;

    string temp_filename;
    char filename[100];

    page = (page_t*)malloc(sizeof(page_t));
    if (page == NULL) printf("malloc error in redo pass");

    offset = 0;

    while (true) {

        // Crash condition
        if (flag == 1 && log_num < ++Log_Num) {
            buf_flush_all();
            fclose(Log_FP);
            Undo_Map.clear();
            Undo_List.clear();
            Winner_Check.clear();
            Next_Undo_LSN.clear();
            free(start_offset);
            free(page);
            return 1;
        }

        memcpy(&type, &Log_Buffer[offset + TYPE_START_POS], TYPE_SIZE);

        /* Case : UPDATE */
        if (type == 1) {

            // Read log from log buffer
            memcpy(&update_log, &Log_Buffer[offset], LOG_U_SIZE);

            if (is_open(update_log.table_id) <= 0) {
                temp_filename = "DATA" + to_string(update_log.table_id);
                strcpy(filename, temp_filename.c_str());
                file_open(filename);
            }

            // Read page from data buffer
            buf_read_page(update_log.table_id, update_log.pagenum, page);

            // Consider Redo : redo only when page LSN is smaller than log LSN
            if (page->p.page_LSN < update_log.lsn) {
                record_index = ((update_log.offset % PAGE_SIZE) - PAGE_HEADER_SIZE) / 128;
                // Update page LSN
                page->p.page_LSN = update_log.lsn;
                // Update value
                memcpy(page->p.l_records[record_index].value, update_log.new_img, IMG_SIZE);
                buf_write_page(update_log.table_id, update_log.pagenum, page);
                fprintf(Log_FP, "LSN %d [UPDATE] Transaction id %d redo apply\n", update_log.lsn + LOG_U_SIZE, g_log.trx_id);
            }

            else
                fprintf(Log_FP, "LSN %d [CONSIDER-REDO] Transaction id %d\n", update_log.lsn + LOG_U_SIZE, update_log.trx_id);

            // Append this log to undo list
            if (loser.count(update_log.trx_id) != 0) {
                Undo_Map[update_log.trx_id].push_back(update_log.lsn);
                Undo_List.push_back(make_pair(update_log.trx_id, update_log.lsn));
                Next_Undo_LSN[compensate_log.trx_id] = compensate_log.next_undo_LSN;
            }

            // Forward offset 288
            offset += LOG_U_SIZE;

        }

        /* Case : COMPENSATE */
        else if (type == 4) {

            // Read log from log buffer
            memcpy(&compensate_log, &Log_Buffer[offset], LOG_C_SIZE);

            // Read page from data buffer
            buf_read_page(compensate_log.table_id, compensate_log.pagenum, page);

            // Consider Redo : redo only when page LSN is smaller than log LSN
            if (page->p.page_LSN < compensate_log.lsn) {
                record_index = ((compensate_log.offset % PAGE_SIZE) - PAGE_HEADER_SIZE) / 128;
                // Update page LSN
                page->p.page_LSN = compensate_log.lsn;
                // Update value
                memcpy(page->p.l_records[record_index].value, compensate_log.new_img, IMG_SIZE);
                buf_write_page(compensate_log.table_id, compensate_log.pagenum, page);

                fprintf(Log_FP, "LSN %d [CLR] next undo lsn %d\n", compensate_log.lsn + LOG_C_SIZE, compensate_log.next_undo_LSN);
//                printf("RECOVERY :: [REDO] trxid: %d, O - %s, N - %s\n", compensate_log.trx_id, compensate_log.old_img, compensate_log.new_img);

            }

            else fprintf(Log_FP, "LSN %d [CONSIDER-REDO] Transaction id %d\n", compensate_log.lsn + LOG_C_SIZE, compensate_log.trx_id);


            // Append this log to undo list
            if (loser.count(compensate_log.trx_id) != 0) {
                Undo_Map[compensate_log.trx_id].push_back(compensate_log.lsn);
                Undo_List.push_back(make_pair(compensate_log.trx_id, compensate_log.lsn));
                Next_Undo_LSN[compensate_log.trx_id] = compensate_log.next_undo_LSN;
            }

            // Forward offset 296
            offset += LOG_C_SIZE;

        }

        /* Case : BEGIN, COMMIT, ROLLBACK Log, forward offset 24 */
        else {

            // Read log from log buffer
            memcpy(&g_log, &Log_Buffer[offset], LOG_G_SIZE);

            if (type == 0) fprintf(Log_FP, "LSN %d [BEGIN] Transaction id %d\n", g_log.lsn + LOG_G_SIZE, g_log.trx_id);

            else if (type == 2) fprintf(Log_FP, "LSN %d [COMMIT] Transaction id %d\n", g_log.lsn + LOG_G_SIZE, g_log.trx_id);

            else fprintf(Log_FP, "LSN %d [ROLLBACK] Transaction id %d\n", g_log.lsn + LOG_G_SIZE, g_log.trx_id);

            offset += LOG_G_SIZE;

        }

        /* Loop termination condition */
        memcpy(&type, &Log_Buffer[offset + TYPE_START_POS], TYPE_SIZE);

        if (read_size - offset == 0) {
            *start_offset += offset;
            break;
        }

        else {

            if (type == 1) {
                if (read_size - offset < LOG_U_SIZE) {
                    *start_offset += offset;
                    break;
                }
            }

            else if (type == 4) {
                if (read_size - offset < LOG_C_SIZE) {
                    *start_offset += offset;
                    break;
                }
            }

            else {
                if (read_size - offset < LOG_G_SIZE) {
                    *start_offset += offset;
                    break;
                }
            }

        }

    }

    free(page);

    return 0;

}


int undo_pass(int flag, int log_num, int * start_offset) {

    page_t *    page;
    logU_t      update_log;
    logC_t      compensate_log;
    logG_t      rollback_log;

    pair<int, int> undo_target;
    unordered_map< int, int >::iterator iter;

    int trx_id;
    int offset;
    int record_index;
    int undo_flag;

    page = (page_t*)malloc(sizeof(page_t));
    if (page == NULL) printf("malloc error in redo pass");

    while (Undo_List.size() != 0) {

        undo_target = Undo_List.back();

        if (undo_target.second < *start_offset) break;

        trx_id = undo_target.first;
        offset = undo_target.second;
        offset -= *start_offset;

        if (Undo_Map[trx_id].back() != undo_target.second)
            printf("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
                   "@@@@@@@@@@@ ERROR 2 @@@@@@@@@@\n"
                   "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
            );

        Undo_Map[trx_id].pop_back();
        Undo_List.pop_back();

        // Decide undo or not
        if (Next_Undo_LSN.count(trx_id) == 0)
            undo_flag = 1;

        else {
            if (Next_Undo_LSN[trx_id] == undo_target.second) {
                if (Undo_Map[trx_id].size() == 0)   Next_Undo_LSN.erase(trx_id);
                else                                Next_Undo_LSN[trx_id] = Undo_Map[trx_id].back();
                undo_flag = 1;
            }

            else if (Next_Undo_LSN[trx_id] == -1) {

                // Read log from log buffer
                memcpy(&update_log, &Log_Buffer[offset], LOG_U_SIZE);

                // Read page from data buffer
                buf_read_page(update_log.table_id, update_log.pagenum, page);

                /* ------------------------ Issue ROLLBACK LOG ------------------------ */
                rollback_log.lsn = issue_LSN(LOG_G_SIZE);
                rollback_log.prev_lsn = get_and_update_last_LSN(trx_id, rollback_log.lsn);
                rollback_log.trx_id = trx_id;
                rollback_log.type = 3;

                if (Temp_Log_Buf_Offset + LOG_G_SIZE > TEMP_LOG_BUFFER_SIZE)
                    write_log(1, 0);

                memcpy(&Temp_Log_Buffer[Temp_Log_Buf_Offset], &rollback_log, LOG_G_SIZE);
                Temp_Log_Buf_Offset += LOG_G_SIZE;
                /* -------------------------------------------------------------------- */

                write_log(1, 0);

                // Update page LSN
                page->p.page_LSN = rollback_log.lsn;

                // Write to data buffer
                buf_write_page(update_log.table_id, update_log.pagenum, page);

                Next_Undo_LSN[trx_id] = -2;

                undo_flag = 0;
            }

            else undo_flag = 0;
        }

        if (undo_flag == 1) {

            // Crash condition
            if (flag == 2 && log_num < ++Log_Num) {
                buf_flush_all();
                fclose(Log_FP);
                Undo_Map.clear();
                Undo_List.clear();
                Winner_Check.clear();
                Next_Undo_LSN.clear();
                free(start_offset);
                free(page);
                return 1;
            }

            // Read log from log buffer
            memcpy(&update_log, &Log_Buffer[offset], LOG_U_SIZE);

            // Read page from data buffer
            buf_read_page(update_log.table_id, update_log.pagenum, page);


            /* ----------------------- Issue COMPENSATE LOG ----------------------- */
            compensate_log.log_size = LOG_C_SIZE;
            compensate_log.lsn = issue_LSN(LOG_C_SIZE);
            compensate_log.prev_lsn = get_and_update_last_LSN(trx_id, compensate_log.lsn);
            compensate_log.trx_id = trx_id;
            compensate_log.type = 4;
            compensate_log.table_id = update_log.table_id;
            compensate_log.pagenum = update_log.pagenum;
            compensate_log.offset = update_log.offset;
            compensate_log.data_length = DATA_LENGTH;
            strcpy(compensate_log.old_img, update_log.new_img);
            strcpy(compensate_log.new_img, update_log.old_img);
            if (Undo_Map[trx_id].size() == 0)
                compensate_log.next_undo_LSN = -1;
            else
                compensate_log.next_undo_LSN = Undo_Map[trx_id].back();

            if (Temp_Log_Buf_Offset + LOG_C_SIZE > TEMP_LOG_BUFFER_SIZE)
                write_log(1, 0);

            memcpy(&Temp_Log_Buffer[Temp_Log_Buf_Offset], &compensate_log, LOG_C_SIZE);
            Temp_Log_Buf_Offset += LOG_C_SIZE;
            /* -------------------------------------------------------------------- */


            // Undo
            record_index = ((update_log.offset % PAGE_SIZE) - PAGE_HEADER_SIZE) / 128;
            // Update page LSN
            page->p.page_LSN = compensate_log.lsn;
            // Undo value
            memcpy(page->p.l_records[record_index].value, update_log.old_img, IMG_SIZE);


            if (compensate_log.next_undo_LSN == -1) {

                /* ------------------------ Issue ROLLBACK LOG ------------------------ */
                rollback_log.lsn = issue_LSN(LOG_G_SIZE);
                rollback_log.prev_lsn = get_and_update_last_LSN(trx_id, rollback_log.lsn);
                rollback_log.trx_id = trx_id;
                rollback_log.type = 3;

                if (Temp_Log_Buf_Offset + LOG_G_SIZE > TEMP_LOG_BUFFER_SIZE)
                    write_log(1, 0);

                memcpy(&Temp_Log_Buffer[Temp_Log_Buf_Offset], &rollback_log, LOG_G_SIZE);
                Temp_Log_Buf_Offset += LOG_G_SIZE;
                /* -------------------------------------------------------------------- */

                write_log(1, 0);

                // Update page LSN
                page->p.page_LSN = rollback_log.lsn;

            }

            // Write to data buffer
            buf_write_page(update_log.table_id, update_log.pagenum, page);

            fprintf(Log_FP ,"LSN %d [UPDATE] Transaction id %d undo apply\n", update_log.lsn + LOG_U_SIZE, update_log.trx_id);

        }

    }

    free(page);

    return 0;

}


void print_log() {

    int type;
    int buf_offset;

    logG_t g_log;
    logU_t u_log;
    logC_t c_log;

    buf_offset = 0;

    while (buf_offset < Log_Buf_Offset) {

        memcpy(&type, &Log_Buffer[buf_offset + TYPE_START_POS], TYPE_SIZE);

        if (type == 0) {

            memcpy(&g_log, &Log_Buffer[buf_offset], LOG_G_SIZE);
            buf_offset += LOG_G_SIZE;

            printf("[BEGIN LOG] LSN : %d, trx_id : %d\n", g_log.lsn, g_log.trx_id);

        }

        else if (type == 2) {

            memcpy(&g_log, &Log_Buffer[buf_offset], LOG_G_SIZE);
            buf_offset += LOG_G_SIZE;

            printf("[COMMIT LOG] LSN : %d, trx_id : %d\n", g_log.lsn, g_log.trx_id);

        }

        else if (type == 3) {

            memcpy(&g_log, &Log_Buffer[buf_offset], LOG_G_SIZE);
            buf_offset += LOG_G_SIZE;

            printf("[ROLLBACK LOG] LSN : %d, trx_id : %d\n", g_log.lsn, g_log.trx_id);

        }

        else if (type == 1) {

            memcpy(&u_log, &Log_Buffer[buf_offset], LOG_U_SIZE);
            buf_offset += LOG_U_SIZE;

            printf("[UPDATE LOG] LSN : %d, trx_id : %d, table_id : %d pagenum : %d\n"
                   "[ -> IMAGES] old image : %s, new image : %s\n",
                   u_log.lsn, u_log.trx_id, u_log.table_id, u_log.pagenum,
                   u_log.old_img, u_log.new_img);

        }

        else {

            memcpy(&c_log, &Log_Buffer[buf_offset], LOG_C_SIZE);
            buf_offset += LOG_C_SIZE;

            printf("[COMPENSATE LOG] LSN : %d, trx_id : %d, table_id : %d pagenum : %d\n"
                   "[ -> IMAGES] old image : %s, new image : %s, next undo LSN : %d\n",
                   c_log.lsn, c_log.trx_id, c_log.table_id, c_log.pagenum,
                   c_log.old_img, c_log.new_img, c_log.next_undo_LSN);

        }

    }

}


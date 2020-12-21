#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#include "db_api.h"
#include "transaction_manager.h"
#include "log_manager.h"
#include "index_manager.h"

int flag = 0;
int log_num = 0;
char log_path[120];
char logmsg_path[120];


#define DATABASE_BUFFER_SIZE	(100)

typedef int			TableId;
typedef int64_t		Key;
typedef int			TransactionId;
typedef char		Value[120];

TableId	table_id_array[20];


/******************************************************************************
 * utils
 */

typedef struct {
    TableId	table_id;
    Key		key;
} TableIdKey;

/* compare pairs of table id & key */
int compare_tik(const void* first, const void* second)
{
    TableIdKey* left = (TableIdKey*) first;
    TableIdKey* right = (TableIdKey*) second;

    if (left->table_id < right->table_id)
        return -1;
    if (left->table_id > right->table_id)
        return 1;
    /* table id is same */
    if (left->key < right->key)
        return -1;
    if (left->key > right->key)
        return 1;
    return 0;
}

/* sort pairs of table id & key */
void
sort_table_id_key(TableId table_ids[], Key keys[], int count)
{
    TableIdKey* tik = (TableIdKey*) malloc(sizeof(TableIdKey) * count);
    /* length of array >= count * 2 */
    for (int i = 0; i < count; i++) {
        tik[i].table_id = table_ids[i];
        tik[i].key = keys[i];
    }
    qsort(tik, count, sizeof(TableIdKey), compare_tik);
    for (int i = 0; i < count; i++) {
        table_ids[i] = tik[i].table_id;
        keys[i] = tik[i].key;
    }
    free(tik);
}


/******************************************************************************
 * single thread test (STT)
 */

#define SST_TABLE_NUMBER		(1)
#define SST_TABLE_SIZE			(100)
#define SST_OPERATION_NUMBER	(100)

pthread_mutex_t SST_mutex;
int64_t SST_operation_count;

void*
SST_func(void* args)
{
    int64_t			operation_count;
    TableId			table_id;
    Key				key1, key2;
    Value			value;
    Value			ret_val;
    TransactionId	transaction_id;
    int				ret;


    for (;;) {
        pthread_mutex_lock(&SST_mutex);
        operation_count = SST_operation_count++;
        pthread_mutex_unlock(&SST_mutex);
        if (operation_count > SST_OPERATION_NUMBER)
            break;

        table_id = table_id_array[rand() % SST_TABLE_NUMBER];
        key1 = rand() % SST_TABLE_SIZE;
        key2 = rand() % SST_TABLE_SIZE;
        sprintf(value, "%ld", key2);

        if (key1 == key2)
            /* Avoid accessing the same record twice. */
            continue;

        transaction_id = trx_begin();

        ret = db_find(table_id, key1, ret_val, transaction_id);
        if (ret != 0) {
            printf("INCORRECT: fail to db_find()\n");
            return NULL;
        }
        if (atoi(ret_val) != 0 && atoi(ret_val) != key1) {
            printf("INCORRECT: value is wrong\n");
            return NULL;
        }

        ret = db_update(table_id, key2, value, transaction_id);
        if (ret != 0) {
            printf("INCORRECT: fail to db_update()\n");
            return NULL;
        }

        trx_commit(transaction_id);
    }

    return NULL;
}

/* simple single thread test */
void
single_thread_test()
{
    pthread_t	thread;
    int64_t		operation_count_0;
    int64_t		operation_count_1;

    /* Initiate variables for test. */
    SST_operation_count = 0;
    pthread_mutex_init(&SST_mutex, NULL);

    /* Initiate database. */
    init_db(DATABASE_BUFFER_SIZE, flag, log_num, "LogFile.db", "LogMessageFile.txt");

    /* open table */
    for (int i = 0; i < SST_TABLE_NUMBER; i++) {
        char* str = (char*) malloc(sizeof(char) * 100);
        TableId table_id;
        sprintf(str, "DATA%d", i + 1);
        table_id = open_table(str);
        table_id_array[i] = table_id;

        /* insertion */
        for (Key key = 0; key < SST_TABLE_SIZE; key++) {
            Value value;
            sprintf(value, "%d", 0);
            db_insert(table_id, key, value);
        }
    }

    printf("database init\n");

    /* thread create */
    pthread_create(&thread, 0, SST_func, NULL);

    for (;;) {
        pthread_mutex_lock(&SST_mutex);
        operation_count_0 = SST_operation_count;
        pthread_mutex_unlock(&SST_mutex);
        if (operation_count_0 > SST_OPERATION_NUMBER)
            break;

        sleep(1);

        pthread_mutex_lock(&SST_mutex);
        operation_count_1 = SST_operation_count;
        pthread_mutex_unlock(&SST_mutex);
        if (operation_count_1 > SST_OPERATION_NUMBER)
            break;

        if (operation_count_0 == operation_count_1) {
            printf("INCORRECT: all threads are working nothing.\n");
        }
    }

    /* thread join */
    pthread_join(thread, NULL);

    /* close table */
    for (int i = 0; i < SST_TABLE_NUMBER; i++) {
        TableId table_id;
        table_id = table_id_array[i];
        close_table(table_id);
    }

    print_log();

    /* shutdown db */
    shutdown_db();
}


/******************************************************************************
 * s-lock test (SLT)
 * s-lock only test
 */

#define SLT_TABLE_NUMBER		(1)
#define SLT_TABLE_SIZE			(100)
#define SLT_THREAD_NUMBER		(10)

#define SLT_FIND_NUMBER			(10)

#define SLT_OPERATION_NUMBER	(100000)

pthread_mutex_t SLT_mutex;
int64_t SLT_operation_count;

void*
SLT_func(void* args)
{
    int64_t			operation_count;
    TableId			table_ids[SLT_FIND_NUMBER];
    Key				keys[SLT_FIND_NUMBER];
    Value			ret_val;
    TransactionId	transaction_id;
    int				ret;


    for (;;) {
        pthread_mutex_lock(&SLT_mutex);
        operation_count = SLT_operation_count++;
        pthread_mutex_unlock(&SLT_mutex);
        if (operation_count > SLT_OPERATION_NUMBER)
            break;

        for (int i = 0; i < SLT_FIND_NUMBER; i++) {
            table_ids[i] = table_id_array[rand() % SLT_TABLE_NUMBER];
            keys[i] = rand() % SLT_TABLE_SIZE;
        }
        sort_table_id_key(table_ids, keys, SLT_FIND_NUMBER);

        /* transaction begin */
        transaction_id = trx_begin();

        for (int i = 0; i < SLT_FIND_NUMBER; i++) {
            if (i != 0 && table_ids[i] == table_ids[i-1] && keys[i] == keys[i-1])
                /* Avoid accessing the same record twice. */
                continue;

            ret = db_find(table_ids[i], keys[i], ret_val, transaction_id);
            if (ret != 0) {
                printf("INCORRECT: fail to db_find()\n", pthread_self());
                return NULL;
            }
            if (atoi(ret_val) != keys[i]) {
                printf("INCORRECT: value is wrong\n");
                printf("value : %d, ret_val : %d\n", pthread_self(), i, keys[i], atoi(ret_val));
                return NULL;
            }
        }

        /* transaction commit */
        trx_commit(transaction_id);
    }

    return NULL;
}

/* s-lock test */
void
slock_test()
{
    pthread_t	threads[SLT_THREAD_NUMBER];
    int64_t		operation_count_0;
    int64_t		operation_count_1;

    /* Initiate variables for test. */
    SLT_operation_count = 0;
    pthread_mutex_init(&SLT_mutex, NULL);

    /* Initiate database. */
    init_db(DATABASE_BUFFER_SIZE, flag, log_num, "LogFile.db", "LogMessageFile.txt");

    /* open table */
    for (int i = 0; i < SLT_TABLE_NUMBER; i++) {
        char* str = (char*) malloc(sizeof(char) * 100);
        TableId table_id;
        sprintf(str, "DATA%d", i + 1);
        table_id = open_table(str);
        table_id_array[i] = table_id;

        /* insertion */
        for (Key key = 0; key < SLT_TABLE_SIZE; key++) {
            Value value;
            sprintf(value, "%ld", key);
            db_insert(table_id, key, value);
        }
    }

    printf("database init\n");

    /* thread create */
    for (int i = 0; i < SLT_THREAD_NUMBER; i++) {
        pthread_create(&threads[i], 0, SLT_func, NULL);
    }

    for (;;) {
        pthread_mutex_lock(&SLT_mutex);
        operation_count_0 = SLT_operation_count;
        pthread_mutex_unlock(&SLT_mutex);
        if (operation_count_0 > SLT_OPERATION_NUMBER)
            break;

        sleep(1);

        pthread_mutex_lock(&SLT_mutex);
        operation_count_1 = SLT_operation_count;
        pthread_mutex_unlock(&SLT_mutex);
        if (operation_count_1 > SLT_OPERATION_NUMBER)
            break;

        if (operation_count_0 == operation_count_1) {
            printf("INCORRECT: all threads are working nothing.\n");
        }
    }

    /* thread join */
    for (int i = 0; i < SLT_THREAD_NUMBER; i++) {
        pthread_join(threads[i], NULL);
    }

    /* close table */
    for (int i = 0; i < SLT_TABLE_NUMBER; i++) {
        TableId table_id;
        table_id = table_id_array[i];
        close_table(table_id);
    }

    /* shutdown db */
    shutdown_db();
}


/******************************************************************************
 * x-lock test (SLT)
 * x-lock only test without deadlock
 */

#define XLT_TABLE_NUMBER		(1)
#define XLT_TABLE_SIZE			(100)
#define XLT_THREAD_NUMBER		(10)

#define XLT_UPDATE_NUMBER		(10)

#define XLT_OPERATION_NUMBER	(100000)

pthread_mutex_t XLT_mutex;
int64_t XLT_operation_count;

void*
XLT_func(void* args)
{
    int64_t			operation_count;
    TableId			table_ids[XLT_UPDATE_NUMBER];
    Key				keys[XLT_UPDATE_NUMBER];
    Value			val;
    TransactionId	transaction_id;
    int				ret;


    for (;;) {
        pthread_mutex_lock(&XLT_mutex);
        operation_count = XLT_operation_count++;
        pthread_mutex_unlock(&XLT_mutex);
        if (operation_count > XLT_OPERATION_NUMBER)
            break;

        for (int i = 0; i < XLT_UPDATE_NUMBER; i++) {
            table_ids[i] = table_id_array[rand() % XLT_TABLE_NUMBER];
            keys[i] = rand() % XLT_TABLE_SIZE;
        }
        /* sorting for avoiding deadlock */
        sort_table_id_key(table_ids, keys, XLT_UPDATE_NUMBER);

        /* transaction begin */
        transaction_id = trx_begin();

        for (int i = 0; i < XLT_UPDATE_NUMBER; i++) {
            if (i != 0 && table_ids[i] == table_ids[i-1] && keys[i] == keys[i-1])
                /* Avoid accessing the same record twice. */
                continue;

            sprintf(val, "%ld", keys[i]);
            ret = db_update(table_ids[i], keys[i], val, transaction_id);
            if (ret != 0) {
                printf("INCORRECT: fail to db_update()\n"
                       "table id : %d, key : %d, value : %d, Trx_id :%d\n"
                       , table_ids[i], keys[i], val, transaction_id
                );
                return NULL;
            }
        }

        if (transaction_id % 2 == 0) {
            printf("COMMIT : %d\n", transaction_id);
            trx_commit(transaction_id);
        }

    }

    return NULL;
}

/* x-lock test */
void
xlock_test()
{
    pthread_t	threads[XLT_THREAD_NUMBER];
    int64_t		operation_count_0;
    int64_t		operation_count_1;

    /* Initiate variables for test. */
    XLT_operation_count = 0;
    pthread_mutex_init(&XLT_mutex, NULL);

    /* Initiate database. */
    init_db(DATABASE_BUFFER_SIZE, flag, log_num, "LogFile.db", "LogMessageFile.txt");

    /* open table */
    for (int i = 0; i < XLT_TABLE_NUMBER; i++) {
        char* str = (char*) malloc(sizeof(char) * 100);
        TableId table_id;
        sprintf(str, "DATA%d", i + 1);
        table_id = open_table(str);
        table_id_array[i] = table_id;

        /* insertion */
        for (Key key = 0; key < XLT_TABLE_SIZE; key++) {
            Value value;
            sprintf(value, "%ld", (Key) 0);
            db_insert(table_id, key, value);
        }
    }

    printf("database init\n");

    /* thread create */
    for (int i = 0; i < XLT_THREAD_NUMBER; i++) {
        pthread_create(&threads[i], 0, XLT_func, NULL);
    }

    for (;;) {
        pthread_mutex_lock(&XLT_mutex);
        operation_count_0 = XLT_operation_count;
        pthread_mutex_unlock(&XLT_mutex);
        if (operation_count_0 > XLT_OPERATION_NUMBER)
            break;

        sleep(1);

        pthread_mutex_lock(&XLT_mutex);
        operation_count_1 = XLT_operation_count;
        pthread_mutex_unlock(&XLT_mutex);
        if (operation_count_1 > XLT_OPERATION_NUMBER)
            break;

        if (operation_count_0 == operation_count_1) {
            printf("INCORRECT: all threads are working nothing.\n");
            //print_lock_table();
        }
    }

    /* thread join */
    for (int i = 0; i < XLT_THREAD_NUMBER; i++) {
        pthread_join(threads[i], NULL);
    }

    /* close table */
    for (int i = 0; i < XLT_TABLE_NUMBER; i++) {
        TableId table_id;
        table_id = table_id_array[i];
        close_table(table_id);
    }

    /* shutdown db */
    shutdown_db();
}


/******************************************************************************
 * mix-lock test (MLT)
 * mix-lock test without deadlock
 */

#define MLT_TABLE_NUMBER		(1)
#define MLT_TABLE_SIZE			(100)
#define MLT_THREAD_NUMBER		(10)

#define MLT_FIND_UPDATE_NUMBER	(20)

#define MLT_OPERATION_NUMBER	(100000)

pthread_mutex_t MLT_mutex;
int64_t MLT_operation_count;

void*
MLT_func(void* args)
{
    int64_t			operation_count;
    TableId			table_ids[MLT_FIND_UPDATE_NUMBER];
    Key				keys[MLT_FIND_UPDATE_NUMBER];
    Value			val;
    Value			ret_val;
    TransactionId	transaction_id;
    int				ret;


    for (;;) {
        pthread_mutex_lock(&MLT_mutex);
        operation_count = MLT_operation_count++;
        pthread_mutex_unlock(&MLT_mutex);
        if (operation_count > MLT_OPERATION_NUMBER)
            break;

        for (int i = 0; i < MLT_FIND_UPDATE_NUMBER; i++) {
            table_ids[i] = table_id_array[rand() % MLT_TABLE_NUMBER];
            keys[i] = rand() % MLT_TABLE_SIZE;
        }
        /* sorting for avoiding deadlock */
        sort_table_id_key(table_ids, keys, MLT_FIND_UPDATE_NUMBER);

        /* transaction begin */
        transaction_id = trx_begin();

        for (int i = 0; i < MLT_FIND_UPDATE_NUMBER; i++) {
            if (i != 0 && table_ids[i] == table_ids[i-1] && keys[i] == keys[i-1])
                /* Avoid accessing the same record twice. */
                continue;

            if (rand() % 2 == 0) {
                /* db_find */
                ret = db_find(table_ids[i], keys[i], ret_val, transaction_id);
                if (ret != 0) {
                    printf("INCORRECT: fail to db_find()\n");
                    return NULL;
                }
                if (keys[i] != 0 && (atoi(ret_val) % keys[i]) != 0) {
                    printf("INCORRECT: value is wrong\n");
                    return NULL;
                }
            } else {
                /* db_update */
                sprintf(val, "%ld", keys[i] * (rand() % 100));
                ret = db_update(table_ids[i], keys[i], val, transaction_id);
                if (ret != 0) {
                    return NULL;
                }
            }

        }

//        /* transaction commit */
//        if (transaction_id % 2 == 0)
            trx_commit(transaction_id);
    }

    return NULL;
}

/* mixed lock test */
void
mlock_test()
{
    pthread_t	threads[MLT_THREAD_NUMBER];
    int64_t		operation_count_0;
    int64_t		operation_count_1;

    /* Initiate variables for test. */
    MLT_operation_count = 0;
    pthread_mutex_init(&MLT_mutex, NULL);

    /* Initiate database. */
    init_db(10, flag, log_num, "LogFile.db", "LogMessageFile.txt");

    /* open table */
    for (int i = 0; i < MLT_TABLE_NUMBER; i++) {
        char* str = (char*) malloc(sizeof(char) * 100);
        TableId table_id;
        sprintf(str, "DATA%d", i + 1);
        table_id = open_table(str);
        table_id_array[i] = table_id;

        /* insertion */
        for (Key key = 0; key < MLT_TABLE_SIZE; key++) {
            Value value;
            sprintf(value, "%ld", (Key) key);
            db_insert(table_id, key, value);
        }
    }

    printf("database init\n");

    /* thread create */
    for (int i = 0; i < MLT_THREAD_NUMBER; i++) {
        pthread_create(&threads[i], 0, MLT_func, NULL);
    }

    for (;;) {
        pthread_mutex_lock(&MLT_mutex);
        operation_count_0 = MLT_operation_count;
        pthread_mutex_unlock(&MLT_mutex);
        if (operation_count_0 > MLT_OPERATION_NUMBER)
            break;

        sleep(1);

        pthread_mutex_lock(&MLT_mutex);
        operation_count_1 = MLT_operation_count;
        pthread_mutex_unlock(&MLT_mutex);
        if (operation_count_1 > MLT_OPERATION_NUMBER)
            break;

        if (operation_count_0 == operation_count_1) {
            printf("INCORRECT: all threads are working nothing.\n");
            print_lock_table();
        }
    }

    /* thread join */
    for (int i = 0; i < MLT_THREAD_NUMBER; i++) {
        pthread_join(threads[i], NULL);
    }

    /* close table */
    for (int i = 0; i < MLT_TABLE_NUMBER; i++) {
        TableId table_id;
        table_id = table_id_array[i];
        close_table(table_id);
    }

    /* shutdown db */
    shutdown_db();
}


/******************************************************************************
 * deadlock test (DLT)
 * mix-lock test with deadlock
 */

#define DLT_TABLE_NUMBER		(1)
#define DLT_TABLE_SIZE			(100)
#define DLT_THREAD_NUMBER		(5)

#define DLT_FIND_UPDATE_NUMBER	(5)

#define DLT_OPERATION_NUMBER	(100000)

pthread_mutex_t DLT_mutex;
int64_t DLT_operation_count;

void*
DLT_func(void* args)
{
    int64_t			operation_count;
    TableId			table_ids[DLT_FIND_UPDATE_NUMBER];
    Key				keys[DLT_FIND_UPDATE_NUMBER];
    Value			val;
    Value			ret_val;
    TransactionId	transaction_id;
    int				ret;
    bool			flag;


    for (;;) {
        pthread_mutex_lock(&DLT_mutex);
        operation_count = DLT_operation_count++;
        pthread_mutex_unlock(&DLT_mutex);
        if (operation_count > DLT_OPERATION_NUMBER)
            break;

        for (int i = 0; i < DLT_FIND_UPDATE_NUMBER; i++) {
            table_ids[i] = table_id_array[rand() % DLT_TABLE_NUMBER];
            keys[i] = rand() % DLT_TABLE_SIZE;
        }

        /* transaction begin */
        transaction_id = trx_begin();

        for (int i = 0; i < DLT_FIND_UPDATE_NUMBER; i++) {
            flag = false;
            for (int j = 0; j < i; j++) {
                if (table_ids[i] == table_ids[j] && keys[i] == keys[j]) {
                    flag = true;
                }
            }
            if (flag == true)
                /* avoid accessing same record twice */
                continue;

            if (rand() % 2 == 0) {
                /* db_find */
                ret = db_find(table_ids[i], keys[i], ret_val, transaction_id);
                if (ret != 0) {
                    /* abort */
                    break;
                }
                if (keys[i] != 0 && (atoi(ret_val) % keys[i]) != 0) {
                    printf("INCORRECT: value is wrong\n");
                    return NULL;
                }
            } else {
                /* db_update */
                sprintf(val, "%ld", keys[i] * (rand() % 100));
                ret = db_update(table_ids[i], keys[i], val, transaction_id);
                if (ret != 0) {
                    /* abort */
                    break;
                }
            }
        }

        trx_commit(transaction_id);
    }

    return NULL;
}

/* dead lock test */
void
deadlock_test()
{
    pthread_t	threads[DLT_THREAD_NUMBER];
    int64_t		operation_count_0;
    int64_t		operation_count_1;

    /* Initiate variables for test. */
    DLT_operation_count = 0;
    pthread_mutex_init(&DLT_mutex, NULL);

    /* Initiate database. */
    init_db(DATABASE_BUFFER_SIZE, flag, log_num, "LogFile.db", "LogMessageFile.txt");

    /* open table */
    for (int i = 0; i < DLT_TABLE_NUMBER; i++) {
        char* str = (char*) malloc(sizeof(char) * 100);
        TableId table_id;
        sprintf(str, "DATA%d", i + 1);
        table_id = open_table(str);
        table_id_array[i] = table_id;

        /* insertion */
        for (Key key = 0; key < DLT_TABLE_SIZE; key++) {
            Value value;
            sprintf(value, "%ld", (Key) key);
            db_insert(table_id, key, value);
        }
    }

    printf("database init\n");

    /* thread create */
    for (int i = 0; i < DLT_THREAD_NUMBER; i++) {
        pthread_create(&threads[i], 0, DLT_func, NULL);
    }

    for (;;) {
        pthread_mutex_lock(&DLT_mutex);
        operation_count_0 = DLT_operation_count;
        pthread_mutex_unlock(&DLT_mutex);
        if (operation_count_0 > DLT_OPERATION_NUMBER)
            break;

        sleep(1);

        pthread_mutex_lock(&DLT_mutex);
        operation_count_1 = DLT_operation_count;
        pthread_mutex_unlock(&DLT_mutex);
        if (operation_count_1 > DLT_OPERATION_NUMBER)
            break;

        if (operation_count_0 == operation_count_1) {
            printf("INCORRECT: all threads are working nothing.\n");
            //print_lock_table();
        }
    }

    /* thread join */
    for (int i = 0; i < DLT_THREAD_NUMBER; i++) {
        pthread_join(threads[i], NULL);
    }

    /* close table */
    for (int i = 0; i < DLT_TABLE_NUMBER; i++) {
        TableId table_id;
        table_id = table_id_array[i];
        close_table(table_id);
    }

    /* shutdown db */
    shutdown_db();
}


/******************************************************************************
 * Main
 */


int main( void ) {

    // Command
    char instruction;

    // Data file
    char pathname[21];
    int table_id;

    // Buffer
    int buf_size;

    // Record (key-value pair)
    int64_t input_key;
    char input_value[120];

    // Return value
    char ret_val[120];

    // For command 'I', 'D'
    char a[120];
    int64_t in_start, in_end, del_start, del_end;

    // For time checking
    time_t start, end;

    // Result of each operation
    int result;

    // Transaction
    int mode;

    // Recovery
    int _flag = 0;
    int _log_num = 0;
    char _log_path[120];
    char _logmsg_path[120];
    int test_cnt = 0;

    /************************** VARIABLES **************************/
    // Initialize DB
    int     BUF_SIZE = 100;
    int     FLAG = 0;
    int     LOG_NUM = 0;
    char    LOG_PATH[20] = {"LOG_FILE.db"};
    char    LOGMSG_PATH[20] = {"LOG_MSG.txt"};

    // Open data file
    char    DATA_PATH[20] = {"DATA1"};
    int     TABLE_ID;

    // Insert values
    char    VALUE[120];

    // Transaction operations
    int     TRX_ID[10];
    int     UPDATE_CNT = 5;
    int     UPDATE_KEY;
    char    UPDATE_VALUE[120] = {"UPDATE"};
    char    UPDATE_VALUE_1[120];
    char    UPDATE_VALUE_2[120];
    /***************************************************************/

    // Usage
    print_usage();
    printf("> ");



    while (scanf("%c", &instruction) != EOF) {

        switch (instruction) {

            case 'B':
                scanf("%d", &buf_size);
                scanf("%d %d %s %s", &_flag, &_log_num, _log_path, _logmsg_path);
                result = init_db(DATABASE_BUFFER_SIZE, _flag, _log_num, _log_path, _logmsg_path);
                if (result == 0) printf("DB initializing is completed\n");
                else if (result == 1) printf("Buffer creation fault\n");
                else if (result == 2) printf("DB is already initialized\nDB initializing fault\n");
                else if (result == 3) printf("Buffer size must be over 0\nDB initializing fault\n");
                else if (result == 4) printf("Lock table initialize error\n");
                else if (result == 5) printf("Transaction mutex error\n");
                else if (result == 6) printf("Recovery error\n");
                else printf("? Error ?\n");
                break;

            case 'O':
                scanf("%s", pathname);
                table_id = open_table(pathname);
                if (table_id == -1) printf("Fail to open file\nFile open fault\n");
                else if (table_id == -2) printf("File name format is wrong\nFile name should be \"DATA00\"\nFile open fault\n");
                else printf("File open is completed\nTable id : %d\n", table_id);
                break;

            case 'R':
                index_init_db(100);
                scanf("%d %d %s %s", &_flag, &_log_num, _log_path, _logmsg_path);
                init_log(_logmsg_path);
                DB_recovery(_flag, _log_num, _log_path);


                break;

            case 'i':
                scanf("%d %ld %[^\n]", &table_id, &input_key, input_value);
                start = clock();
                result = db_insert(table_id, input_key, input_value);
                end = clock();
                if (result == 0) {
                    printf("Insertion is completed\n");
                    printf("Time : %f\n", (double)(end - start));
                }
                else if (result == 1) printf("Table_id[%d] file is not exist\n", table_id);
                else if (result == 2) printf("Duplicate key <%ld>\nInsertion fault\n", input_key);
                break;

            case 'd':
                scanf("%d %ld", &table_id, &input_key);
                start = clock();
                result = db_delete(table_id, input_key);
                end = clock();
                if (result == 0) {
                    printf("Deletion is completed\n");
                    printf("Time : %f\n", (double)(end - start));
                }
                else if (result == 1) printf("Table_id[%d] file is not exist\n", table_id);
                else if (result == 2) printf("No such key <%ld>\nDeletion fault\n", input_key);
                break;

            case 'u':
                /***************************** TEST ****************************/
                // Initialize DB
                init_db(BUF_SIZE, FLAG, LOG_NUM, LOG_PATH, LOGMSG_PATH);
                printf("SUCCESS :: INITIALIZE DB\n");

                // Open data file
                TABLE_ID = open_table(DATA_PATH);
                printf("SUCCESS :: OPEN DATA FILE\n");

                // Insert values
                for (int64_t i = 0; i <= 1000; i++) {
                    sprintf(VALUE, "%ld", i);
                    db_insert(TABLE_ID, i, VALUE);
                }
                printf("SUCCESS :: INSERT VALUES\n");

                // Transaction operations
                for (int i = 0; i < 10; i++) {

                    TRX_ID[i] =trx_begin();

                    for (int j = 0; j < UPDATE_CNT; j++) {
                        UPDATE_KEY = rand() % 1000 + 1;
                        strcpy(UPDATE_VALUE_1, UPDATE_VALUE);
                        sprintf(UPDATE_VALUE_2, "%d", UPDATE_KEY);
                        strcat(UPDATE_VALUE_1, UPDATE_VALUE_2);
                        db_update(TABLE_ID, UPDATE_KEY, UPDATE_VALUE_1, TRX_ID[i]);
                    }

                    // COMMIT Transaction 1, 3, 5, 7
                    if (TRX_ID[i] == 1 || TRX_ID[i] == 3 || TRX_ID[i] == 5 || TRX_ID[i] == 7)
                        trx_commit(TRX_ID[i]);

                    // ABORT Transaction 2, 4, 6, 10
                    else if (TRX_ID[i] == 2 || TRX_ID[i] == 4 || TRX_ID[i] == 6 || TRX_ID[i] == 10)
                        trx_abort(TRX_ID[i]);

                    // Do not COMMIT OR ABORT Transaction 8, 9
                    else
                        continue;

                    // FLUSH DATA BUFFER TO DATA DISK
                    if (TRX_ID[i] == 4)
                        db_flush(TABLE_ID);

                }
                printf("SUCCESS :: TEST\n");
                /***************************************************************/
                break;

            case 'L':
                write_log(0, 0);
                break;

             case 'I':
                scanf("%d %ld %ld", &table_id, &in_start, &in_end);
                strcpy(a, "a");
                start = clock();
                for (int64_t i = in_start; i <= in_end; i++) {
                    sprintf(a, "%ld", i);
                    result = db_insert(table_id, i, a);
                    if (result == 2) printf("Duplicate key <%ld>\nInsertion fault\n", i);
                }
                end = clock();
                printf("Time : %f\n", (double)(end - start));
                break;

            case 'D':
                scanf("%d %ld %ld", &table_id, &del_start, &del_end);
                start = clock();
                for (int64_t i = del_start; i <= del_end; i++) {
					result = db_delete(table_id, i);
                    if (result == 2) printf("No such key <%ld>\nDeletion fault\n", i);
                }
                end = clock();
                printf("Time : %f\n", (double)(end - start));
                break;

            case 'T':
                srand(123);

                scanf("%d", &mode);

                if (mode == 1) {
                    single_thread_test();
                }
                else if (mode == 2) {
                    slock_test();
                }
                else if (mode == 3) {
                    xlock_test();
                }
                else if (mode == 4) {
                    mlock_test();
                }
                else if (mode == 5) {
                    deadlock_test();
                }
                break;

            case 't':
                db_print_table_list();
                break;

            case 'p':
                scanf("%d", &table_id);
                db_print(table_id);
                break;

            case 'l':
                scanf("%d", &table_id);
                db_print_leaf(table_id);
                break;

            case 'C':
                scanf("%d", &table_id);
                result = close_table(table_id);
                if (result == 0) printf("Close is completed\n");
                else if (result == 1) printf("File having table_id[%d] is not exist\nClose fault\n", table_id);
                else printf("Close fault\n");
                break;

            case 'S':
                result = shutdown_db();
                if (result == 0) printf("Shutdown is completed\n");
                else if (result == 1) printf("Buffer is not exist\nShutdown is completed\n");
                else printf("Shutdown fault\n");
                break;

            case 'Q':
                while (getchar() != (int)'\n');
                result = shutdown_db();
                if (result == 0) printf("Shutdown is completed\n");
                else if (result == 1) printf("Buffer is not exist\nShutdown is completed\n");
                else printf("Shutdown fault\n");
                return EXIT_SUCCESS;

            case 'F':
                scanf("%d", &table_id);
                db_flush(table_id);
                break;

            case 'U':
                print_usage();
                break;

            default:
                printf("Invalid Command\n");
                break;
        }

        while (getchar() != (int)'\n');
        printf("> ");

    }

    printf("\n");

    return EXIT_SUCCESS;
}

#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include "db_api.h"
#include "transaction_manager.h"

#define THREAD_CNT (3)
#define TABLE_NUMBER (3)
#define KEY_NUMBER (200)
#define TEST_CNT (5)

// For buffer hit ratio
double hit_cnt = 0, miss_cnt = 0;


void * trx_func(void * args) {
    int trx_id, table_id, result;
    int64_t key;
    char ret_val[120];
    char change_val[120] = "UPDATED5";

    trx_id = trx_begin();

    printf("trx_id : %d\n", trx_id);

//    for (int i = 0; i < TEST_CNT; i++) {
//        printf("-----------------db_find--------------------\n");
//        table_id = rand() % TABLE_NUMBER + 1;
//        key = rand() % KEY_NUMBER;
//        printf("TEST - table_id : %d, key : %d, Thread id : %u\n", table_id, key, pthread_self());
//        result = db_find(table_id, key, ret_val, trx_id);
//        if (result == 0) printf("Success db_find\nret_val : %s, Thread id : %u\n", ret_val, pthread_self());
//        else if (result == 1){
//            printf("Abort db_find\n");
//            break;
//        }
//    }

     if (result == 0) {
         printf("-----------------db_update--------------------\n");
         for (int i = 0; i < TEST_CNT; i++) {
             table_id = rand() % TABLE_NUMBER + 1;
             key = rand() % KEY_NUMBER;
             printf("TEST - table_id : %d, key : %d, Thread id : %u\n", table_id, key, pthread_self());
             result = db_update(table_id, key, change_val, trx_id);

             if (result == 0) printf("Success db_update, Thread id : %u\n", pthread_self());
             else if (result == 1){
                 printf("Abort db_update, Thead id : %u\n", pthread_self());
                 break;
             }
         }
     }
    
//    if (result == 0) {
//        printf("-----------------db_find--------------------\n");
//        for (int i = 0; i < TEST_CNT; i++) {
//            table_id = rand() % TABLE_NUMBER + 1;
//            key = rand() % KEY_NUMBER;
//            printf("TEST - table_id : %d, key : %d, Thread id : %u\n", table_id, key, pthread_self());
//            result = db_find(table_id, key, ret_val, trx_id);
//            if (result == 0) printf("Success db_find\nret_val : %s, Thread id : %u\n", ret_val, pthread_self());
//            else if (result == 1){
//                printf("Abort db_find\n");
//                break;
//            }
//        }
//    }

    if (result == 0) {
        printf("before commit\n");
        trx_commit(trx_id);
        printf("after commit\n");
    }
    else if (result == 3) printf("Abort has been happened, No commit\n");
    
    printf("Thread is done.\n");

    return NULL;
}


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
    srand(time(NULL));
    pthread_t user_threads[THREAD_CNT];
    
    // Usage
    print_usage();
    printf("> ");

    while (scanf("%c", &instruction) != EOF) {

        switch (instruction) {

            case 'B':
                scanf("%d", &buf_size);
                result = init_db(buf_size);
                if (result == 0) printf("DB initializing is completed\n");
                else if (result == 1) printf("Buffer creation fault\n");
                else if (result == 2) printf("DB is already initialized\nDB initializing fault\n");
                else if (result == 3) printf("Buffer size must be over 0\nDB initializing fault\n");
                break;

            case 'O':
                scanf("%s", pathname);
                table_id = open_table(pathname);
                if (table_id == -1) printf("Fail to open file\nFile open fault\n");
                else if (table_id == -2) printf("Can not open more than 10 tables\nFile open fault\n");
                else printf("File open is completed\nTable id : %d\n", table_id);
                break;

            case 'i':
                scanf("%d %ld %[^\n]", &table_id, &input_key, input_value);
                start = clock();
                result = db_insert(table_id, input_key, input_value);
                end = clock();
                if (result == 0) {
                    printf("Insertion is completed\n");
                    printf("Time : %f\n", (double)(end - start));
                    printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
                }
                else if (result == 1) printf("Table_id[%d] file is not exist\n", table_id);
                else if (result == 2) printf("Duplicate key <%ld>\nInsertion fault\n", input_key);
                break;

            // case 'f':
            //     scanf("%d %ld", &table_id, &input_key);
            //     start = clock();
            //     result = db_find(table_id, input_key, ret_val);
            //     end = clock();
            //     if (result == 0) {
            //         printf("table_id : %d, key : %ld, value : %s\nFind is completed\n", table_id, input_key, ret_val);
            //         printf("Time : %f\n", (double)(end - start));
            //         printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
            //     }
            //     else if (result == 1) printf("Table_id[%d] file is not exist\n", table_id);
            //     else if (result == 2) printf("No such key\nFind fault\n");
            //     break;

            case 'd':
                scanf("%d %ld", &table_id, &input_key);
                start = clock();
                result = db_delete(table_id, input_key);
                end = clock();
                if (result == 0) {
                    printf("Deletion is completed\n");
                    printf("Time : %f\n", (double)(end - start));
                    printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
                }
                else if (result == 1) printf("Table_id[%d] file is not exist\n", table_id);
                else if (result == 2) printf("No such key <%ld>\nDeletion fault\n", input_key);
                break;

             case 'I':
                scanf("%d %ld %ld", &table_id, &in_start, &in_end);
                strcpy(a, "a");
                start = clock();
                for (int64_t i = in_start; i <= in_end; i++) {
                    result = db_insert(table_id, i, a);
                    if (result == 2) printf("Duplicate key <%ld>\nInsertion fault\n", i);
                }
                end = clock();
                printf("Time : %f\n", (double)(end - start));
                printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
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
                printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
                break;

            case 'T':
                for (int i = 0; i < THREAD_CNT; i++)
                    pthread_create(&user_threads[i], 0, trx_func, NULL);

                for (int i = 0; i < THREAD_CNT; i++)
                    pthread_join(user_threads[i], NULL);

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
                break;

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

        hit_cnt = 0;
        miss_cnt = 0;

        while (getchar() != (int)'\n');
        printf("> ");

    }

    printf("\n");

    return EXIT_SUCCESS;
}

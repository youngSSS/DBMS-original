#include <stdio.h>
#include <time.h>
#include "db_api.h"
#include "buffer_manager.h"

double hit_cnt = 0, miss_cnt = 0;

int main( void ) {

    char instruction;
    char pathname[1000];
    char input_value[120];
    char ret_val[120];
    int64_t input_key;
    int table_id;
    int result;
    int buf_size;

    // For command 'I', 'D'
    char a[120];
    int64_t in_start, in_end, del_start, del_end;

    time_t start, end;

    // Usage
    printf(
    "Enter any of the following commands after the prompt > :\n"
    "\to <pathname>  -- Oepn <pathname> file\n"
    "\tb <num>  -- Make buffer with size <num>\n"
    "\ti <table_id> <key> <value>  -- Insert <key> <value>\n"
    "\tf <table_id> <key>  -- Find the value under <key>\n"
    "\td <table_id> <key>  -- Delete key <key> and its associated value\n"
    "\tI <table_id> <num> <num>  -- Insert <num> ~ <num>\n"
    "\tD <table_id> <num> <num>  -- Delete <num> ~ <num>\n"
    "\tp <table_id>  -- Print the data file in B+ tree structure\n"
    "\tl <table_id>  -- Print all leaf records\n"
    "\ts  -- Destroy buffer\n"
    "\tc <table_id>  -- Close file having <table_id>\n"
    "\tq  -- Quit\n"
    );
    
    printf("> ");

    page_t* temp = (page_t*)malloc(sizeof(page_t));

    while (scanf("%c", &instruction) != EOF) {

        switch (instruction) {

            case 'o':
                scanf("%s", pathname);
                table_id = open_table(pathname);
                if (table_id == -1) printf("Fail to open file\nFile open fault\n");
                else if (table_id == -2) printf("Can not open more than 10 tables\nFile open fault\n");
                else printf("File open is completed\nTable id : %d\n", table_id);
                break;

            case 'b':
                scanf("%d", &buf_size);
                init_db(buf_size);
                break;

            case 'i':
                scanf("%d %lld %s", &table_id, &input_key, input_value);
                start = clock();
                result = db_insert(table_id, input_key, input_value);
                end = clock();
                printf("Time : %f\n", (double)(end - start));
                if (result == 0) printf("Insertion is completed\n");
                else if (result == 1) printf("File is not opened yet\nOpen the file\n");
                else if (result == 2) printf("Duplicate key\nInsertion fault\n");

                printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
                hit_cnt = 0;
                miss_cnt = 0;

                break;

            case 'f':
                scanf("%d %lld", &table_id, &input_key);
                result = db_find(table_id, input_key, ret_val);
                if (result == 0) printf("table_id : %d, key : %lld, value : %s\nFind is completed\n", 
                    table_id, input_key, ret_val);
                else if (result == 1) printf("File is not opened yet\nOpen the file\n");
                else if (result == 2) printf("No such key\nFind fault");
                //db_print(table_id);

                printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
                hit_cnt = 0;
                miss_cnt = 0;

                break;

            case 'd':
                scanf("%d %lld", &table_id, &input_key);
                result = db_delete(table_id, input_key);
                if (result == 0) printf("Deletion is completed\n");
                else if (result == 1) printf("File is not opened yet\nOpen the file\n");
                else if (result == 2) printf("No such key\nDeletion fault\n");
                //db_print(table_id);

                printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
                hit_cnt = 0;
                miss_cnt = 0;

                break;

             case 'I':
                scanf("%d %lld %lld", &table_id, &in_start, &in_end);
                strcpy(a, "a");
                start = clock();

                for (int64_t i = in_start; i <= in_end; i++)
                    db_insert(table_id, i, a);
                    
                end = clock();
                printf("Time : %f\n", (double)(end - start));

                printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
                hit_cnt = 0;
                miss_cnt = 0;

                break;

            case 'D':
                scanf("%d %lld %lld", &table_id, &del_start, &del_end);
                start = clock();
                for (int i = del_start; i <= del_end; i++)
                    db_delete(table_id, i);
                end = clock();
                printf("Time : %f\n", (double)(end - start));

                printf("Hit ratio : %f\n", hit_cnt / (hit_cnt + miss_cnt) * 100);
                hit_cnt = 0;
                miss_cnt = 0;
                
                //db_print(table_id);
                break;

            case 'p':
                scanf("%d", &table_id);
                db_print(table_id);
                break;

            case 'l':
                scanf("%d", &table_id);
                db_print_leaf(table_id);
                break;

            case 's':
                result = shutdown_db();
                if (result == 0) printf("Shutdown is completed\n");
                else if (result == 1) printf("Buffer is not exist\nShutdown is completed\n");
                else printf("Shutdown fault\n");
                break;

            case 'c':
                result = close_table(table_id);
                if (result == 0) printf("Close is completed\n");
                else if (result == 1) printf("File having table_id is not exist\nClose fault\n");
                else printf("Close fault\n");
                break;

            case 'q':
                while (getchar() != (int)'\n');
                return EXIT_SUCCESS;
                break;

            default:
                break;
        }

        while (getchar() != (int)'\n');
        printf("> ");

    }

    printf("\n");

    return EXIT_SUCCESS;
}

#include <stdio.h>
#include "db_api.h"

int main( void ) {

    char instruction;
    char pathname[1000];
    char input_value[120];
    char ret_val[120];
    int input_key;
    int result;
    char a[120];
    int in_num, del_num;

    // Usage
    printf("Enter any of the following commands after the prompt > :\n"
    "\to <pathname>  -- Oepn <pathname> file\n"
    "\ti <key> <value>  -- Insert <key> <value>\n"
    "\tf <key>  -- Find the value under <key>\n"
    "\td <key>  -- Delete key <key> and its associated value\n"
    "\tp  -- Print the data file in B+ tree structure\n"
    "\tl  -- Print all leaf records\n"
    "\tI <num>  -- Insert <num> ~ 1\n"
    "\tD <num>  -- Delete 1 ~ <num - 1>\n"
    "\tc  -- Close file\n"
    "\tq  -- Quit\n");
    
    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'o':
            scanf("%s", pathname);
            Unique_table_id = open_table(pathname);
            if (Unique_table_id < 0) printf("Fail to open file\nFile open fault\n");
            else printf("File open is completed\n");
            break;
        case 'I':
            scanf("%d", &in_num);
            strcpy(a, "a");
            for (int i = 1; i <= in_num; i++)
                db_insert(i, a);
            break;
        case 'D':
            scanf("%d", &del_num);
            for (int i = 1; i <= del_num; i++)
                db_delete(i);
            db_print();
            break;
        case 'i':
            scanf("%d %s", &input_key, input_value);
            result = db_insert(input_key, input_value);
            if (result == 0) printf("Insertion is completed\n");
            else if (result == 1) printf("File is not opened yet\nOpen the file\n");
            else if (result == 2) printf("Duplicate key\nInsertion fault\n");
            db_print();
            break;
        case 'f':
            scanf("%d", &input_key);
            result = db_find(input_key, ret_val);
            if (result == 0) printf("key : %d, value : %s\nFind is completed\n", input_key, ret_val);
            else if (result == 1) printf("File is not opened yet\nOpen the file\n");
            else if (result == 2) printf("No such key\nFind fault");
            db_print();
            break;
        case 'd':
            scanf("%d", &input_key);
            result = db_delete(input_key);
            if (result == 0) printf("Deletion is completed\n");
            else if (result == 1) printf("File is not opened yet\nOpen the file\n");
            else if (result == 2) printf("No such key\nDeletion fault\n");
            db_print();
            break;
        case 'p':
            db_print();
            break;
        case 'l':
            db_print_leaf();
            break;
        case 'c':
            if (db_close(Unique_table_id) == 0)
                printf("File close is completed\n");
            else printf("Close table fault\n");
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

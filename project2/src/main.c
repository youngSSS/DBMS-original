#include <stdio.h>
#include "db_api.h"

int main( void ) {

    char instruction;
    char pathname[1000];
    char input_value[120];
    char ret_val[120];
    int input_key;
    int result;

    // Usage
    printf("Enter any of the following commands after the prompt > :\n"
    "\to <pathname>  -- Oepn <pathname> file\n"
    "\ti <key> <value>  -- Insert <key> <value>\n"
    "\tf <key>  -- Find the value under <key>\n"
    "\td <key>  -- Delete key <key> and its associated value\n"
    "\tp  -- Print the data file in B+ tree structure\n"
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
        case 'i':
            scanf("%d %s", &input_key, input_value);
            result = db_insert(input_key, input_value);
            if (result != 0) printf("Duplicate key\nInsertion fault\n");
            else printf("Insertion is completed.\n");
            db_print();
            break;
        case 'f':
            scanf("%d", &input_key);
            result = db_find(input_key, ret_val);
            if (result != 0) printf("No such key\nFind fault");
            else {
                printf("key : %d, value : %s\n", input_key, ret_val);
                printf("Find is completed\n");
            }
            db_print();
            break;
        case 'd':
            scanf("%d", &input_key);
            result = db_delete(input_key);
            if (result != 0) printf("No such key\nDeletion fault\n");
            else printf("Deletion is completed\n");
            db_print();
            break;
        case 'p':
            db_print();
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

/* B+ Tree */

#include "bpt.h"


// Global variable

int leaf_order = DEFAULT_LEAF_ORDER;
int internal_order = DEFAULT_INTERNAL_ORDER;


// Queue

pagenum_t queue[MAX];
int front = -1;
int rear = -1;
int q_size = 0;

int IsEmpty() {
    if (front == rear) return 1;
    else return 0;
}

int IsFull(){
    if ((rear + 1) % MAX == front) return 1;
    else return 0;
}

void enqueue(pagenum_t pagenum) {
    if ( !IsFull() ) {
        rear = (rear + 1) % MAX;
        queue[rear] = pagenum;
        q_size++;
    }
}

pagenum_t dequeue(){
    if (IsEmpty()) return 0;
    else {
        q_size--;
        front = (front + 1) % MAX;
        return queue[front];
    }
}


// Open

int index_open(char * pathname) {
    return open_file(pathname);
}

int index_check_file_size(int unique_table_id) {
    return check_file_size(unique_table_id);
}


// Print

void print_leaf() {
    page_t* page = make_page();
    pagenum_t temp_pagenum;
    int i = 0;

    if (header_page->h.root_pagenum == 0) {
        printf("Tree is empty\n");
        return;
    }

    file_read_page(header_page->h.root_pagenum, page);

    while (!page->p.is_leaf) {
        temp_pagenum = page->p.one_more_pagenum;
        file_read_page(temp_pagenum, page);
    }

    while (1) {
        for (int j = 0; j < page->p.num_keys; j++)
            printf("%lu ", page->p.l_records[j].key);
        printf("| ");

        if (page->p.right_sibling_pagenum == 0)
            break;

        file_read_page(page->p.right_sibling_pagenum, page);
    }

    printf("\n");

    free(page);
}


void print_file() {
    page_t* page = make_page();
    int i = 0;
    int err_cnt = 0;

    if (header_page->h.root_pagenum == 0) {
        printf("Tree is empty\n");
        return;
    }

    enqueue(header_page->h.root_pagenum);

    printf("\n");

    while (!IsEmpty()) {
        int temp_size = q_size;

        while (temp_size) {
            pagenum_t pagenum = dequeue();

            file_read_page(pagenum, page);

            if (page->p.is_leaf) {
                for (i = 0; i < page->p.num_keys; i++) {
                    printf("(%ld, %s) ", page->p.l_records[i].key, page->p.l_records[i].value);
                }
                printf(" | ");
            }

            else {
                printf("[%lu] ", page->p.one_more_pagenum);
                
                enqueue(page->p.one_more_pagenum);

                for (i = 0; i < page->p.num_keys; i++) {
                    printf("%ld [%lu] ", page->p.i_records[i].key, page->p.i_records[i].pagenum);
                    enqueue(page->p.i_records[i].pagenum);
                }

                printf(" | ");
            }

            temp_size--;
        }
        printf("\n");
    }

    enqueue(header_page->h.root_pagenum);

    printf("\n");

    while (!IsEmpty()) {
        int temp_size = q_size;

        while (temp_size) {
            pagenum_t pagenum = dequeue();

            file_read_page(pagenum, page);

            if (page->p.is_leaf) {
                printf("pagenum : %lu, parent : %lu, is_leaf : %d, num keys : %d, right sibling : %lu", 
                    pagenum, page->p.parent_pagenum, page->p.is_leaf, page->p.num_keys, page->p.right_sibling_pagenum);
                printf(" | ");
            }

            else {
                printf("pagenum : %lu, parent : %lu, is_leaf : %d, num keys : %d, one more : %lu", 
                    pagenum, page->p.parent_pagenum, page->p.is_leaf, page->p.num_keys, page->p.one_more_pagenum);
                
                enqueue(page->p.one_more_pagenum);
                
                for (i = 0; i < page->p.num_keys; i++)
                    enqueue(page->p.i_records[i].pagenum);

                printf(" | ");
            }

            temp_size--;
        }
        printf("\n");
    }

    printf("\n");

    free(page);
}


void find_and_print(uint64_t key) {
    leafRecord * r = find(key);
    if (r == NULL) printf("Record not found under key %lu.\n", key);
    else printf("Record -- key %lu, value %s.\n", key, r->value);
    free(r);
}


/* Find */

page_t * find_leaf_page(uint64_t key) {
    int i = 0;
    pagenum_t root_pagenum;
    page_t * page;

    root_pagenum = header_page->h.root_pagenum;

    /* Case : Empty file */
    if (root_pagenum == 0) return NULL;
    
    page = make_page();
    file_read_page(root_pagenum, page);

    while (!page->p.is_leaf) {
        i = 0;

        while (i < page->p.num_keys) {
            if (key >= page->p.i_records[i].key) i++;
            else break;
        }

        if (i == 0) 
            file_read_page(page->p.one_more_pagenum, page);
        else
            file_read_page(page->p.i_records[i - 1].pagenum, page);
    }

    return page;
}


leafRecord * find(uint64_t key) {
    int i = 0;

    page_t * leaf_page = find_leaf_page(key);
    leafRecord * leaf_record = (leafRecord*)malloc(sizeof(leafRecord));

    // Case : Empty file
    if (leaf_page == NULL) return NULL;

    for (i = 0; i < leaf_page->p.num_keys; i++)
        if (leaf_page->p.l_records[i].key == key){
            leaf_record->key = leaf_page->p.l_records[i].key;
            strcpy(leaf_record->value, leaf_page->p.l_records[i].value);
            free(leaf_page);
            return leaf_record;
        }

    free(leaf_page);

    return NULL;
}


/* Etc */

int cut( int length ) {
    if (length % 2 == 0)
        return length / 2;
    else
        return length / 2 + 1;
}


/* Insertion */

leafRecord make_leaf_record(uint64_t key, char * value) {
    leafRecord new_record;

    new_record.key = key;
    strcpy(new_record.value, value);
    
    return new_record;
}


page_t * make_page( void ) {
    page_t* new_page;
    new_page = (page_t*)malloc(sizeof(page_t));

    if (new_page == NULL) {
        perror("Page creation.");
        exit(EXIT_FAILURE);
    }

    new_page->p.parent_pagenum = 0;
    new_page->p.is_leaf = 0;
    new_page->p.num_keys = 0;

    return new_page;
}


page_t * make_leaf_page( void ) {
    page_t * leaf_page = make_page();
    leaf_page->p.is_leaf = 1;
    return leaf_page;
}


int get_left_index(page_t * parent, page_t * left) {

    int left_index = 0;
    pagenum_t left_pagenum = get_pagenum(left);

    if (parent->p.one_more_pagenum == left_pagenum)
        return -1;

    for (left_index = 0; left_index < parent->p.num_keys; left_index++) {
        if (parent->p.i_records[left_index].pagenum == left_pagenum)
            break;
    }

    return left_index;
}


int insert_into_leaf( page_t* leaf_page, uint64_t key, leafRecord leaf_record ) {

    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < leaf_page->p.num_keys && leaf_page->p.l_records[insertion_point].key < key)
        insertion_point++;

    for (i = leaf_page->p.num_keys; i > insertion_point; i--) 
        leaf_page->p.l_records[i] = leaf_page->p.l_records[i - 1];

    leaf_page->p.l_records[insertion_point] = leaf_record;
    leaf_page->p.num_keys++;

    file_write_page(get_pagenum(leaf_page), leaf_page);

    free(leaf_page);

    return 1;
}


int insert_into_leaf_after_splitting(page_t * leaf_page, uint64_t key, leafRecord leaf_record) {

    page_t * new_leaf_page;
    pagenum_t new_leaf_pagenum;
    leafRecord temp_records[leaf_order];
    int insertion_index, split, new_key, i, j;

    new_leaf_page = make_leaf_page();

    insertion_index = 0;

    for (insertion_index = 0; insertion_index < leaf_page->p.num_keys; insertion_index++) {
        if (key < leaf_page->p.l_records[insertion_index].key)
            break;
    }

    for (i = 0, j = 0; i < leaf_page->p.num_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_records[j] = leaf_page->p.l_records[i];
    }

    temp_records[insertion_index] = leaf_record;

    leaf_page->p.num_keys = 0;

    split = cut(leaf_order - 1);

    for (i = 0; i < split; i++) {
        leaf_page->p.l_records[i] = temp_records[i];
        leaf_page->p.num_keys++;
    }

    for (i = split, j = 0; i < leaf_order; i++, j++) {
        new_leaf_page->p.l_records[j] = temp_records[i];
        new_leaf_page->p.num_keys++;
    }

    new_leaf_pagenum = file_alloc_page();

    new_leaf_page->p.right_sibling_pagenum = leaf_page->p.right_sibling_pagenum;
    leaf_page->p.right_sibling_pagenum = new_leaf_pagenum;

    new_leaf_page->p.parent_pagenum = leaf_page->p.parent_pagenum;
    new_key = new_leaf_page->p.l_records[0].key;

    file_write_page(get_pagenum(leaf_page), leaf_page);
    file_write_page(new_leaf_pagenum, new_leaf_page);

    return insert_into_parent(leaf_page, new_key, new_leaf_page, new_leaf_pagenum);
}


int insert_into_page(page_t * parent, int left_index, uint64_t key, pagenum_t right_pagenum) {
    int i;

    for (i = parent->p.num_keys; i > left_index + 1 ; i--)
        parent->p.i_records[i] = parent->p.i_records[i - 1];

    parent->p.i_records[left_index + 1].key = key;
    parent->p.i_records[left_index + 1].pagenum = right_pagenum;
    parent->p.num_keys++;

    file_write_page(get_pagenum(parent), parent);

    free(parent);

    return 1;
}


int insert_into_page_after_splitting(page_t * old_page, int left_index, int64_t key, pagenum_t right_pagenum) {

    int i, j, split, k_prime;
    page_t * new_page, * child_page;
    pagenum_t new_pagenum, child_pagenum;
    internalRecord temp_records[internal_order];

    for (i = 0, j = 0; i < old_page->p.num_keys; i++, j++) {
        if (j == left_index + 1) j++;
        temp_records[j] = old_page->p.i_records[i];
    }

    temp_records[left_index + 1].key = key;
    temp_records[left_index + 1].pagenum = right_pagenum;

    split = cut(internal_order);

    new_page = make_page();
    new_pagenum = file_alloc_page();

    // old_page set
    old_page->p.num_keys = 0;

    for (i = 0; i < split - 1; i++) {
        old_page->p.i_records[i] = temp_records[i];
        old_page->p.num_keys++;
    }

    // Set k_prime
    k_prime = temp_records[split - 1].key;

    // new_page set
    new_page->p.one_more_pagenum = temp_records[split - 1].pagenum;

    for (++i, j = 0; i < internal_order; i++, j++) {
        new_page->p.i_records[j] = temp_records[i];
        new_page->p.num_keys++;
    }
    
    new_page->p.parent_pagenum = old_page->p.parent_pagenum;

    file_write_page(get_pagenum(old_page), old_page);
    file_write_page(new_pagenum, new_page);

    // Change parent of new_page's children
    child_page = make_page();

    file_read_page(new_page->p.one_more_pagenum, child_page);
    child_page->p.parent_pagenum = new_pagenum;
    file_write_page(new_page->p.one_more_pagenum, child_page);

    for (i = 0; i < new_page->p.num_keys; i++) {
        child_pagenum = new_page->p.i_records[i].pagenum;
        file_read_page(child_pagenum, child_page);
        child_page->p.parent_pagenum = new_pagenum;
        file_write_page(child_pagenum, child_page);
    }

    free(child_page);

    return insert_into_parent(old_page, k_prime, new_page, new_pagenum);
}


int insert_into_parent(page_t * left, uint64_t key, page_t * right, pagenum_t right_pagenum) {

    int left_index;
    page_t * parent;

    if (left->p.parent_pagenum == 0)
        return insert_into_new_root(left, key, right, right_pagenum);

    parent = make_page();
    file_read_page(left->p.parent_pagenum, parent);

    left_index = get_left_index(parent, left);

    free(left);
    free(right);

    if (parent->p.num_keys < internal_order - 1)
        return insert_into_page(parent, left_index, key, right_pagenum);

    return insert_into_page_after_splitting(parent, left_index, key, right_pagenum);
}


int insert_into_new_root(page_t * left, uint64_t key, page_t * right, pagenum_t right_pagenum) {

    page_t * root = make_page();
    pagenum_t root_pagenum, left_pagenum;

    left_pagenum = get_pagenum(left);

    root->p.one_more_pagenum = left_pagenum;
    root->p.i_records[0].key = key;
    root->p.i_records[0].pagenum = right_pagenum;
    root->p.num_keys++;
    root->p.parent_pagenum = 0;

    root_pagenum = file_alloc_page();

    header_page->h.root_pagenum = root_pagenum;
    left->p.parent_pagenum = root_pagenum;
    right->p.parent_pagenum = root_pagenum;

    file_write_page(0, header_page);
    file_write_page(left_pagenum, left);
    file_write_page(right_pagenum, right);
    file_write_page(root_pagenum, root);

    free(left);
    free(right);
    free(root);

    return 1;
}


void start_new_tree(uint64_t key, leafRecord leaf_record) {
    pagenum_t pagenum;
    page_t * root;

    root = make_leaf_page();
    root->p.parent_pagenum = 0;
    root->p.right_sibling_pagenum = 0;
    root->p.l_records[0] = leaf_record;
    root->p.num_keys++;

    pagenum = file_alloc_page();

    header_page->h.root_pagenum = pagenum;

    file_write_page(pagenum, root);
	file_write_page(0, header_page);

    free(root);
}


/* Master insertion function */
int insert(uint64_t key, char* value) {

    leafRecord * duplicate_flag;
    leafRecord leaf_record;
    page_t* leaf_page;

    duplicate_flag = find(key);

    // The current implementation ignores duplicates
    if (duplicate_flag != NULL) {
        free(duplicate_flag);
        return 1;
    }

    free(duplicate_flag);

    // Create a new record for the value
    leaf_record = make_leaf_record(key, value);

    /* Case : the tree does not exist yet */

    if (header_page->h.root_pagenum == 0) {
        start_new_tree(key, leaf_record);
        return 0;
    }

    leaf_page = find_leaf_page(key);

    /* Case : leaf has room for record */

    if (leaf_page->p.num_keys < leaf_order - 1)
        insert_into_leaf(leaf_page, key, leaf_record);

    /* Case : leaf has no room for record */
    else
        insert_into_leaf_after_splitting(leaf_page, key, leaf_record);
    
    return 0;
}


/* Deletion */

page_t * remove_entry_from_page(page_t * page, int key_index) {

    int i, num_pointers;

    if (page->p.is_leaf) {
        for (i = key_index + 1; i < page->p.num_keys; i++)
            page->p.l_records[i - 1] = page->p.l_records[i];
    }

    else {
        for (i = key_index + 1; i < page->p.num_keys; i++)
            page->p.i_records[i - 1] = page->p.i_records[i];
    }

    page->p.num_keys--;

    file_write_page(get_pagenum(page), page);

    return page;
}


page_t * adjust_root(page_t * root_page) {

    page_t * new_root_page;
    pagenum_t root_pagenum;

    /* Case: nonempty root */

    if (root_page->p.num_keys > 0)
        return header_page;

    /* Case: empty root */

    root_pagenum = get_pagenum(root_page);

    // If it has a child, promote the first (only) child as the new root.
    if (!root_page->p.is_leaf) {
        new_root_page = make_page();

        file_read_page(root_page->p.one_more_pagenum, new_root_page);
        header_page->h.root_pagenum = root_page->p.one_more_pagenum;
        new_root_page->p.parent_pagenum = 0;
        
        file_write_page(header_page->h.root_pagenum, new_root_page);
        free(new_root_page);
    }

    // If it is a leaf, then the whole tree is empty.
    else
        header_page->h.root_pagenum = 0;

    file_write_page(0, header_page);
    file_free_page(root_pagenum);

    free(root_page);

    return header_page;
}


// Always key_page's num_keys + neighbor's num_keys < (internal or leaf)order - 1
// This means one_more_pagenum is not used when coalesce
page_t * coalesce_nodes(page_t * parent, page_t * key_page, page_t * neighbor, int neighbor_index, int k_prime) {

    page_t * tmp, * temp_page;
    pagenum_t key_pagenum, neighbor_pagenum, temp_pagenum;
    int i, j, neighbor_insertion_index, n_end, key_index;

    if (neighbor_index == -2) {
        tmp = key_page;
        key_page = neighbor;
        neighbor = tmp;
    }

    key_pagenum = get_pagenum(key_page);
    neighbor_pagenum = get_pagenum(neighbor);

    neighbor_insertion_index = neighbor->p.num_keys;

    /* Case : internal page */

    if (!key_page->p.is_leaf) {
        temp_page = make_page();

        neighbor->p.i_records[neighbor_insertion_index].key = k_prime;
        neighbor->p.num_keys++; 

        neighbor->p.i_records[neighbor_insertion_index].pagenum = key_page->p.one_more_pagenum;

        file_read_page(neighbor->p.i_records[neighbor_insertion_index].pagenum, temp_page);
        temp_page->p.parent_pagenum = neighbor_pagenum;
        file_write_page(neighbor->p.i_records[neighbor_insertion_index].pagenum, temp_page);

        if (neighbor_index == -2) {
            for (i = neighbor_insertion_index + 1, j = 0; j < key_page->p.num_keys; i++, j++) {
                neighbor->p.i_records[i] = key_page->p.i_records[j];
                neighbor->p.num_keys++;

                file_read_page(neighbor->p.i_records[i].pagenum, temp_page);
                temp_page->p.parent_pagenum = neighbor_pagenum;
                file_write_page(neighbor->p.i_records[i].pagenum, temp_page);
            }
        }

        free(temp_page);
    }

    /* Case : leaf page */

    else {
        if (neighbor_index == -2) {
            for (i = neighbor_insertion_index, j = 0; j < key_page->p.num_keys; i++, j++) {
                neighbor->p.l_records[i] = key_page->p.l_records[j];
                neighbor->p.num_keys++; 
            }
        }
        
        neighbor->p.right_sibling_pagenum = key_page->p.right_sibling_pagenum;
        
    }

    for (i = 0; i < parent->p.num_keys; i++) {
        if (parent->p.i_records[i].pagenum == key_pagenum) {
            key_index = i;
            break;
        }
    }

    file_write_page(neighbor_pagenum, neighbor);
    file_free_page(key_pagenum);

    free(key_page);
    free(neighbor);

    delete_entry(parent, key_index);
    
    return header_page;
}


page_t * redistribute_nodes(page_t * parent, page_t * key_page, page_t * neighbor, int neighbor_flag, 
        int k_prime_index, int k_prime) {  

    // Only internal page can reach this function
    
    page_t * temp_page;
    pagenum_t key_pagenum, neighbor_pagenum, parent_pagenum;
    int i, j;
    int move_start_index;
    int move_cnt;
    int num_neighbor_keys;

    key_pagenum = get_pagenum(key_page);
    neighbor_pagenum = get_pagenum(neighbor);
    parent_pagenum = get_pagenum(parent);

    num_neighbor_keys = neighbor->p.num_keys;

    // Number of keys which will be passed to key_page
    move_cnt = num_neighbor_keys % 2 == 0 ? (num_neighbor_keys / 2) - 1 : (num_neighbor_keys / 2);

    // The start index of key which will be passed to key_page
    move_start_index = (num_neighbor_keys / 2) + 1;

    /* Case: neighbor is left sibling of key_page */

    if (neighbor_flag != -2) {

        // Move key_page's 0th pagenum to the end 
        // Move k_prime to end of key_page to maintain tree property
        // End means last (pagenum or key) of key_page after redistribution
        key_page->p.i_records[move_cnt].pagenum = key_page->p.one_more_pagenum;
        key_page->p.i_records[move_cnt].key = k_prime;
        key_page->p.num_keys++;

        // Take a records from the neighbor to key_page
        for (i = 0; i < move_cnt; i++) {
            key_page->p.i_records[i] = neighbor->p.i_records[i + move_start_index];
            key_page->p.num_keys++;
            neighbor->p.num_keys--;
        }

        key_page->p.one_more_pagenum = neighbor->p.i_records[move_start_index - 1].pagenum;

        // Take a k_prime from neighbor to parent
        parent->p.i_records[k_prime_index].key = neighbor->p.i_records[move_start_index - 1].key;
        neighbor->p.num_keys--;

        temp_page = make_page();

        // Change parent of key_page's children
        file_read_page(key_page->p.one_more_pagenum, temp_page);
        temp_page->p.parent_pagenum = key_pagenum;
        file_write_page(key_page->p.one_more_pagenum, temp_page);

        for (i = 0; i < key_page->p.num_keys; i++) {
            file_read_page(key_page->p.i_records[i].pagenum, temp_page);
            temp_page->p.parent_pagenum = key_pagenum;
            file_write_page(key_page->p.i_records[i].pagenum, temp_page);
        }

        free(temp_page);
    }

    /* Case: neighbor is right sibling of key_page */

    else {  

        key_page->p.i_records[0].key = k_prime;
        key_page->p.num_keys++;

        // Take a records from the neighbor to key_page
        key_page->p.i_records[0].pagenum = neighbor->p.one_more_pagenum;
        for (i = 0; i < move_cnt; i++) {
            key_page->p.i_records[i + 1] = neighbor->p.i_records[i];
            key_page->p.num_keys++;
            neighbor->p.num_keys--;
        }

        // Reset k_prime
        parent->p.i_records[k_prime_index].key = neighbor->p.i_records[i].key;
        neighbor->p.num_keys--;

        // Rearrangement neighbor
        neighbor->p.one_more_pagenum = neighbor->p.i_records[i].pagenum;
        for (++i, j = 0; i < num_neighbor_keys; i++, j++) {
            neighbor->p.i_records[j] = neighbor->p.i_records[i];
        }

        temp_page = make_page();

        // Change parent of key_page's children
        for (i = 0; i < key_page->p.num_keys; i++) {
            file_read_page(key_page->p.i_records[i].pagenum, temp_page);
            temp_page->p.parent_pagenum = key_pagenum;
            file_write_page(key_page->p.i_records[i].pagenum, temp_page);
        }

        free(temp_page);
    }

    file_write_page(parent_pagenum, parent);
    file_write_page(key_pagenum, key_page);
    file_write_page(neighbor_pagenum, neighbor);

    free(parent);
    free(key_page);
    free(neighbor);

    return header_page;
}


int get_neighbor_index(page_t * parent, page_t * key_page) {
    pagenum_t key_pagenum;
    int i, neighbor_index;

    key_pagenum = get_pagenum(key_page);

    if (parent->p.one_more_pagenum == key_pagenum)
        return -2;

    for (i = 0; i < parent->p.num_keys; i++) {
        if (parent->p.i_records[i].pagenum == key_pagenum) {
            neighbor_index = i - 1;
            break;
        }
    }

    return neighbor_index;
}


page_t * delete_entry(page_t * key_page, int key_index) {

    page_t * parent, * neighbor;
    pagenum_t neighbor_pagenum;
    int neighbor_index, neighbor_flag;
    int k_prime_index, k_prime;

    // Remove key and pointer from node.
    key_page = remove_entry_from_page(key_page, key_index);

    /* Case : deletion from the root */

    if (key_page->p.parent_pagenum == 0)
        return adjust_root(key_page);

    /* Case : Delayed merge  */

    // If page has a key, do nothing
    if (key_page->p.num_keys != 0) {
        free(key_page);
        return header_page;
    }

    // get_neighbor index function

    parent = make_page();
    file_read_page(key_page->p.parent_pagenum, parent);

    key_index = get_neighbor_index(parent, key_page);

    // neighbor is a sibling of key_page
    // If key_page has only right sibling, neighbor_flag is -2
    // If key_page has left sibling in one_more_pagenum, neighbor_flag is -1
    // Otherwise, key_index is neighbor's index in parent
    neighbor_flag = key_index;
    neighbor_index = neighbor_flag == -2 ? 0 : neighbor_flag;

    if (neighbor_index == -1) 
        neighbor_pagenum = parent->p.one_more_pagenum;
    else 
        neighbor_pagenum = parent->p.i_records[neighbor_index].pagenum;

    neighbor = make_page();
    file_read_page(neighbor_pagenum, neighbor);

    // k_prime_index is 0 when left sibling is not exist
    // k_prime_index is neighbor_index when neighbor is exist
    k_prime_index = neighbor_flag == -2 ? 0 : neighbor_flag + 1;

    // k_prime is key value between 
    // neighbor page pointer and key_page pointer in parent page 
    k_prime = parent->p.i_records[k_prime_index].key;

    /* Case : key_page is leaf page */

    if (key_page->p.is_leaf)
        return coalesce_nodes(parent, key_page, neighbor, neighbor_flag, k_prime);

    /* Case : key_page is internal page */

    else {
        if (neighbor->p.num_keys == internal_order - 1)
            return redistribute_nodes(parent, key_page, neighbor, neighbor_flag, k_prime_index, k_prime);

        else
            return coalesce_nodes(parent, key_page, neighbor, neighbor_flag, k_prime);
    }
}


/* Master deletion function */
int delete(uint64_t key) {

    page_t * key_leaf_page;
    leafRecord * key_record;
    int key_index;

    key_leaf_page = find_leaf_page(key);
    key_record = find(key);

    if (key_record == NULL) {
        free(key_record);
        return 1;
    }
    
    if (key_record != NULL && key_leaf_page != NULL) {
        free(key_record);
        for (int i = 0; i < key_leaf_page->p.num_keys; i++) {
            if (key_leaf_page->p.l_records[i].key == key) {
                key_index = i;
                break;
            }
        }

        header_page = delete_entry(key_leaf_page, key_index);
    }

    return 0;
}


/* Help Functions */

pagenum_t get_pagenum(page_t* page) {
    pagenum_t parent_pagenum, pagenum;
    page_t* parent_page;
    int64_t my_key;
    int i = 0;


    parent_pagenum = page->p.parent_pagenum;

    if (parent_pagenum == 0)
        return header_page->h.root_pagenum;

    if (page->p.is_leaf) 
        my_key = page->p.l_records[0].key;
    else 
        my_key = page->p.i_records[0].key;

    parent_page = make_page();
    file_read_page(parent_pagenum, parent_page);

    for (i = 0; i < parent_page->p.num_keys; i++) {
        if (parent_page->p.i_records[i].key > my_key)
            break;
    }

    if (!parent_page->p.is_leaf && i == 0) 
        pagenum = parent_page->p.one_more_pagenum;
    else 
        pagenum = parent_page->p.i_records[i - 1].pagenum;

    free(parent_page);

    return pagenum;
}

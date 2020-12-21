/* Index Manager API */

#include "index_manager.h"
#include "buffer_manager.h"
#include "lock_manager.h"
#include "transaction_manager.h"
#include "log_manager.h"


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


/* --- For layered architecture --- */

// Buffer

int index_init_db(int buf_num) {
    if (init_lock_table() == 1)
        return 4;

    return buf_init_db(buf_num);
}


int index_shutdown_db( void ) {
    return buf_shutdown_db();
}


void index_flush(int table_id) {
    buf_flush(table_id);
}


// File

int index_open(char * pathname) {
    return buf_open(pathname);
}


int index_close(int table_id) {
    return buf_close(table_id);
}


int index_is_open(int table_id) {
    return buf_is_open(table_id);
}


void index_print_table_list() {
    buf_print_table_list();
}


/* ------- Transaction APIs ------- */

// return 0 : find success
// return 1 : no key
// return 2 : abort

int trx_find(int table_id, int64_t key, char * ret_val, int trx_id) {

    pthread_mutex_t *   page_latch;
    lock_t *            lock_obj;
    page_t *            page;
    pagenum_t           pagenum;
    int                 result;
    int                 i;

    /* -------------------- Find Leaf Page -------------------- */
    page = make_page();
    buf_read_page(table_id, 0, page);
    pagenum = page->h.root_pagenum;

    /* Case : key is not exist */
    if (pagenum == 0) {
        free(page);
        return 1;
    }

    // Read root page
    buf_read_page(table_id, pagenum, page);

    while (!page->p.is_leaf) {
        i = 0;

        while (i < page->p.num_keys) {
            if (key >= page->p.i_records[i].key) i++;
            else break;
        }

        if (i == 0) pagenum = page->p.one_more_pagenum;
        else pagenum = page->p.i_records[i - 1].pagenum;

        buf_read_page(table_id, pagenum, page);
    }
    /* -------------------------------------------------------- */

    // Get page latch
    page_latch = mutex_buf_read(table_id, pagenum, page, 0);

    for (i = 0; i < page->p.num_keys; i++) {

        if (key == page->p.l_records[i].key) {

            // Acquire lock
            result = lock_acquire(table_id, page->p.l_records[i].key, trx_id, 0, &lock_obj, page_latch);

            /* Case : Success to acquire lock */
            if (result == 0) {

                /* We have PAGE LATCH & LOCK LATCH */

                // Read again
                mutex_buf_read(table_id, pagenum, page, 1);

                // Do the DB operation
                strcpy(ret_val, page->p.l_records[i].value);

                // Release page latch
                pthread_mutex_unlock(page_latch);

                free(page);
                return 0;

            }

            /* Case : Fail to acquire lock - Wait */
            else if (result == 1) {

                /* We have PAGE LATCH & TRX LATCH */

                // Release page latch & Sleep
                pthread_mutex_unlock(page_latch);
                lock_wait(lock_obj);

                /* We have LOCK OBJECT LATCH */

                // Get page latch
                page_latch = mutex_buf_read(table_id, pagenum, page, 0);

                /* We have PAGE LATCH & LOCK OBJECT LATCH */

                // Do the DB operation
                strcpy(ret_val, page->p.l_records[i].value);

                // Release page latch
                pthread_mutex_unlock(page_latch);

                free(page);
                return 0;

            }

            /* Case : Deadlock */
            else if (result == 2) {

                // Release page latch
                pthread_mutex_unlock(page_latch);

                free(page);
                return 2;

            }

            else {
                if (result == 3) printf("malloc error in lock manager.cpp\n");
                else printf("lock_acquire error (Must not be happen error\n");
            }

        }

    }

    /* Case : key is not exist */
    free(page);

    return 1;
}


// return 0 : update success
// return 1 : no key
// return 2 : abort

int trx_update(int table_id, int64_t key, char * value, int trx_id) {

    pthread_mutex_t *   page_latch;
    lock_t *            lock_obj;
    page_t *            page;
    pagenum_t           pagenum;
    int64_t             lsn;
    int                 result;
    int                 i;

    /* -------------------- Find Leaf Page -------------------- */
    page = make_page();
    buf_read_page(table_id, 0, page);
    pagenum = page->h.root_pagenum;

    /* Case : key is not exist */
    if (pagenum == 0) {
        free(page);
        return 1;
    }

    // Read root page
    buf_read_page(table_id, pagenum, page);

    while (!page->p.is_leaf) {
        i = 0;

        while (i < page->p.num_keys) {
            if (key >= page->p.i_records[i].key) i++;
            else break;
        }

        if (i == 0) pagenum = page->p.one_more_pagenum;
        else pagenum = page->p.i_records[i - 1].pagenum;

        buf_read_page(table_id, pagenum, page);
    }
    /* -------------------------------------------------------- */

    // Get page latch
    page_latch = mutex_buf_read(table_id, pagenum, page, 0);

    for (i = 0; i < page->p.num_keys; i++) {

        if (key == page->p.l_records[i].key) {

            // Acquire lock
            result = lock_acquire(table_id, page->p.l_records[i].key, trx_id, 1, &lock_obj, page_latch);

            /* Case : Success to acquire lock */
            if (result == 0) {

                /* We have PAGE LATCH & LOCK OBJECT LATCH */

                // Read again
                mutex_buf_read(table_id, pagenum, page, 1);

                // Issue a UPDATE Log
                lsn = issue_update_log(trx_id, table_id, pagenum, (pagenum * PAGE_SIZE) + PAGE_HEADER_SIZE + (128 * i),
                                                 page->p.l_records[i].value, value);

                // Logging to transaction
                trx_logging(table_id, key, trx_id, lsn, pagenum, i, page->p.l_records[i].value, value);

                // Do the DB operation
                strcpy(page->p.l_records[i].value, value);
                // Update page LSN
                page->p.page_LSN = lsn;

                // Write updated page to buffer
                mutex_buf_write(table_id, pagenum, page, 1);

                // Release page latch
                pthread_mutex_unlock(page_latch);

                free(page);
                return 0;

            }

            /* Case : Fail to acquire lock - Wait */
            else if (result == 1) {

                /* We have PAGE LATCH & TRX LATCH */

                // Release page latch & Sleep
                pthread_mutex_unlock(page_latch);
                lock_wait(lock_obj);

                /* We have LOCK OBJECT LATCH */

                // Get page latch
                page_latch = mutex_buf_read(table_id, pagenum, page, 0);

                /* We have PAGE LATCH & LOCK OBJECT LATCH */

                // Issue a UPDATE Log
                lsn = issue_update_log(trx_id, table_id, pagenum, (pagenum * PAGE_SIZE) + PAGE_HEADER_SIZE + (128 * i),
                                       page->p.l_records[i].value, value);

                // Logging to transaction
                trx_logging(table_id, key, trx_id, lsn, pagenum, i, page->p.l_records[i].value, value);

                // Do the DB operation
                strcpy(page->p.l_records[i].value, value);
                // Update page LSN
                page->p.page_LSN = lsn;

                // Write updated page to buffer
                mutex_buf_write(table_id, pagenum, page, 1);

                // Release page latch
                pthread_mutex_unlock(page_latch);

                free(page);
                return 0;

            }

            /* Case : Deadlock */
            else if (result == 2) {

                // Release page latch
                pthread_mutex_unlock(page_latch);

                free(page);
                return 2;

            }

            else {
                if (result == 3) printf("malloc error in lock manager.cpp\n");
                else printf("lock_acquire error (Must not be happen error\n");
            }

        }

    }

    /* Case : key is not exist */
    free(page);

    return 1;
}


int undo(int table_id, pagenum_t pagenum, int64_t key, char * old_value, int compensate_lsn) {
    page_t * page;
    pthread_mutex_t * page_latch;
    int i;

    page = make_page();
    page_latch = mutex_buf_read(table_id, pagenum, page, 0);

    for (i = 0; i < page->p.num_keys; i++) {
        if (key == page->p.l_records[i].key){
            page->p.page_LSN = compensate_lsn;
            strcpy(page->p.l_records[i].value, old_value);
            mutex_buf_write(table_id, pagenum, page, 1);
            pthread_mutex_unlock(page_latch);
            free(page);
            return 0;
        }
    }

    /* Case : Error */
    free(page);

    return 1;
}


/* ---------- Index APIs ---------- */

// Print

void print_leaf(int table_id) {
    page_t * header_page, * page;
    pagenum_t temp_pagenum;
    int i = 0;
    int64_t value;

    header_page = make_page();
    file_read_page(table_id, 0, header_page);

    if (header_page->h.root_pagenum == 0) {
        printf("Tree is empty\n");
        free(header_page);
        return;
    }

    page = make_page();
    file_read_page(table_id, header_page->h.root_pagenum, page);

    while (!page->p.is_leaf) {
        temp_pagenum = page->p.one_more_pagenum;
        file_read_page(table_id, temp_pagenum, page);
    }

    while (1) {
        for (int j = 0; j < page->p.num_keys; j++) {
            value = atoi(page->p.l_records[j].value);

            if (page->p.l_records[j].key != value)
                printf("(%ld, %s) ", page->p.l_records[j].key, page->p.l_records[j].value);
        }

        printf("| ");

        if (page->p.right_sibling_pagenum == 0)
            break;

        file_read_page(table_id, page->p.right_sibling_pagenum, page);
    }

    printf("\n");

    free(header_page);
    free(page);
}


void print_file(int table_id) {
    page_t * header_page, * page;
    int i = 0;

    header_page = make_page();
    file_read_page(table_id, 0, header_page);

    if (header_page->h.root_pagenum == 0) {
        printf("Tree is empty\n");
        free(header_page);
        return;
    }

    page = make_page();

    enqueue(header_page->h.root_pagenum);

    printf("\n");

    while (!IsEmpty()) {
        int temp_size = q_size;

        while (temp_size) {
            pagenum_t pagenum = dequeue();

            file_read_page(table_id, pagenum, page);

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

            file_read_page(table_id, pagenum, page);

            if (page->p.is_leaf) {
                printf("pagenum : %lu, parent : %lu, is_leaf : %u, num keys : %u, right sibling : %lu",
                       pagenum, page->p.parent_pagenum, page->p.is_leaf, page->p.num_keys, page->p.right_sibling_pagenum);
                printf(" | ");
            }

            else {
                printf("pagenum : %lu, parent : %lu, is_leaf : %u, num keys : %u, one more : %lu",
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

    free(header_page);
    free(page);
}


/* Find */

p_pnum find_leaf_page(int table_id, pagenum_t root_pagenum, int64_t key) {
    int i = 0;
    page_t * page = NULL;
    pagenum_t pagenum;

    /* Case : Empty file */
    if (root_pagenum == 0) return make_pair(page, root_pagenum);

    page = make_page();
    pagenum = root_pagenum;
    buf_read_page(table_id, pagenum, page);

    while (!page->p.is_leaf) {
        i = 0;

        while (i < page->p.num_keys) {
            if (key >= page->p.i_records[i].key) i++;
            else break;
        }

        if (i == 0) {
            pagenum = page->p.one_more_pagenum;
            buf_read_page(table_id, pagenum, page);
        }
        else {
            pagenum = page->p.i_records[i - 1].pagenum;
            buf_read_page(table_id, pagenum, page);
        }
    }

    return make_pair(page, pagenum);
}


leafRecord * find(int table_id, pagenum_t root_pagenum, int64_t key) {

    p_pnum leaf_pair;
    page_t * leaf_page;
    leafRecord * leaf_record;
    int i = 0;

    leaf_pair = find_leaf_page(table_id, root_pagenum, key);

    leaf_page = leaf_pair.first;

    // Case : Empty file
    if (leaf_page == NULL) return NULL;

    leaf_record = (leafRecord*)malloc(sizeof(leafRecord));

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


int _find(int table_id, int64_t key, char * ret_val) {
    page_t * header_page;
    pagenum_t root_pagenum;
    leafRecord * leaf_record;

    header_page = make_page();
    buf_read_page(table_id, 0, header_page);
    root_pagenum = header_page->h.root_pagenum;
    free(header_page);

    leaf_record = find(table_id, root_pagenum, key);

    if (leaf_record == NULL) return 2;

    strcpy(ret_val, leaf_record->value);

    free(leaf_record);

    return 0;
}


/* Insertion */

leafRecord make_leaf_record(int64_t key, char * value) {
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
    new_page->p.page_LSN = -1;

    return new_page;
}


page_t * make_leaf_page( void ) {
    page_t * leaf_page = make_page();
    leaf_page->p.is_leaf = 1;
    return leaf_page;
}


int get_left_index(int table_id, page_t * parent, pagenum_t left_pagenum) {

    int left_index = 0;

    if (parent->p.one_more_pagenum == left_pagenum)
        return -1;

    for (left_index = 0; left_index < parent->p.num_keys; left_index++) {
        if (parent->p.i_records[left_index].pagenum == left_pagenum)
            break;
    }

    return left_index;
}


void insert_into_leaf(int table_id, p_pnum leaf_pair, int64_t key, leafRecord leaf_record) {

    page_t * leaf_page;
    pagenum_t leaf_pagenum;
    int i, insertion_point = 0;

    leaf_page = leaf_pair.first;
    leaf_pagenum = leaf_pair.second;

    while (insertion_point < leaf_page->p.num_keys && leaf_page->p.l_records[insertion_point].key < key)
        insertion_point++;

    for (i = leaf_page->p.num_keys; i > insertion_point; i--)
        leaf_page->p.l_records[i] = leaf_page->p.l_records[i - 1];

    leaf_page->p.l_records[insertion_point] = leaf_record;
    leaf_page->p.num_keys++;

    buf_write_page(table_id, leaf_pagenum, leaf_page);

    free(leaf_page);
}


int insert_into_leaf_after_splitting(int table_id, p_pnum leaf_pair, int64_t key, leafRecord leaf_record) {

    page_t * leaf_page, * new_leaf_page;
    pagenum_t leaf_pagenum, new_leaf_pagenum, parent_pagenum;
    leafRecord temp_records[leaf_order];
    int insertion_index, split, new_key, i, j;

    leaf_page = leaf_pair.first;
    leaf_pagenum = leaf_pair.second;

    parent_pagenum = leaf_page->p.parent_pagenum;

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

    new_leaf_pagenum = buf_alloc_page(table_id);

    new_leaf_page->p.right_sibling_pagenum = leaf_page->p.right_sibling_pagenum;
    leaf_page->p.right_sibling_pagenum = new_leaf_pagenum;

    new_leaf_page->p.parent_pagenum = parent_pagenum;
    new_key = new_leaf_page->p.l_records[0].key;

    buf_write_page(table_id, leaf_pagenum, leaf_page);
    buf_write_page(table_id, new_leaf_pagenum, new_leaf_page);

    free(leaf_page);
    free(new_leaf_page);

    return insert_into_parent(table_id, parent_pagenum, leaf_pagenum, new_key, new_leaf_pagenum);
}


int insert_into_page(int table_id, p_pnum parent_pair, int left_index, int64_t key, pagenum_t right_pagenum) {

    page_t * parent;
    pagenum_t parent_pagenum;
    int i;

    parent = parent_pair.first;
    parent_pagenum = parent_pair.second;

    for (i = parent->p.num_keys; i > left_index + 1 ; i--)
        parent->p.i_records[i] = parent->p.i_records[i - 1];

    parent->p.i_records[left_index + 1].key = key;
    parent->p.i_records[left_index + 1].pagenum = right_pagenum;
    parent->p.num_keys++;

    buf_write_page(table_id, parent_pagenum, parent);

    free(parent);

    return 1;
}


int insert_into_page_after_splitting(int table_id, p_pnum old_pair, int left_index, int64_t key, pagenum_t right_pagenum) {

    page_t * old_page, * new_page, * child_page;
    pagenum_t old_pagenum, new_pagenum, parent_pagenum;
    internalRecord temp_records[internal_order];
    int i, j, split, k_prime;

    old_page = old_pair.first;
    old_pagenum = old_pair.second;

    parent_pagenum = old_page->p.parent_pagenum;

    for (i = 0, j = 0; i < old_page->p.num_keys; i++, j++) {
        if (j == left_index + 1) j++;
        temp_records[j] = old_page->p.i_records[i];
    }

    temp_records[left_index + 1].key = key;
    temp_records[left_index + 1].pagenum = right_pagenum;

    split = cut(internal_order);

    new_page = make_page();
    new_pagenum = buf_alloc_page(table_id);

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

    new_page->p.parent_pagenum = parent_pagenum;

    buf_write_page(table_id, old_pagenum, old_page);
    buf_write_page(table_id, new_pagenum, new_page);

    // Change parent of new_page's children
    child_page = make_page();

    buf_read_page(table_id, new_page->p.one_more_pagenum, child_page);
    child_page->p.parent_pagenum = new_pagenum;
    buf_write_page(table_id, new_page->p.one_more_pagenum, child_page);

    for (i = 0; i < new_page->p.num_keys; i++) {
        buf_read_page(table_id, new_page->p.i_records[i].pagenum, child_page);
        child_page->p.parent_pagenum = new_pagenum;
        buf_write_page(table_id, new_page->p.i_records[i].pagenum, child_page);
    }

    free(child_page);
    free(old_page);
    free(new_page);

    return insert_into_parent(table_id, parent_pagenum, old_pagenum, k_prime, new_pagenum);
}


int insert_into_parent(int table_id, pagenum_t parent_pagenum, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {

    int left_index;
    page_t * parent;

    if (parent_pagenum == 0)
        return insert_into_new_root(table_id, left_pagenum, key, right_pagenum);

    parent = make_page();
    buf_read_page(table_id, parent_pagenum, parent);

    left_index = get_left_index(table_id, parent, left_pagenum);

    if (parent->p.num_keys < internal_order - 1)
        return insert_into_page(table_id, make_pair(parent, parent_pagenum), left_index, key, right_pagenum);

    return insert_into_page_after_splitting(table_id, make_pair(parent, parent_pagenum), left_index, key, right_pagenum);
}


int insert_into_new_root(int table_id, pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {

    page_t * header_page, * root, * left, * right;
    pagenum_t root_pagenum;

    root = make_page();

    root->p.one_more_pagenum = left_pagenum;
    root->p.i_records[0].key = key;
    root->p.i_records[0].pagenum = right_pagenum;
    root->p.num_keys++;
    root->p.parent_pagenum = 0;

    root_pagenum = buf_alloc_page(table_id);

    header_page = make_page();
    left = make_page();
    right = make_page();

    buf_read_page(table_id, 0, header_page);
    buf_read_page(table_id, left_pagenum, left);
    buf_read_page(table_id, right_pagenum, right);

    header_page->h.root_pagenum = root_pagenum;
    left->p.parent_pagenum = root_pagenum;
    right->p.parent_pagenum = root_pagenum;

    buf_write_page(table_id, 0, header_page);
    buf_write_page(table_id, left_pagenum, left);
    buf_write_page(table_id, right_pagenum, right);
    buf_write_page(table_id, root_pagenum, root);

    free(header_page);
    free(left);
    free(right);
    free(root);

    return 1;
}


int start_new_tree(int table_id, int64_t key, leafRecord leaf_record) {
    page_t * header_page, * root;
    pagenum_t root_pagenum;

    root = make_leaf_page();
    root->p.parent_pagenum = 0;
    root->p.right_sibling_pagenum = 0;
    root->p.l_records[0] = leaf_record;
    root->p.num_keys++;

    root_pagenum = buf_alloc_page(table_id);
    header_page = make_page();
    buf_read_page(table_id, 0, header_page);

    header_page->h.root_pagenum = root_pagenum;

    buf_write_page(table_id, 0, header_page);
    buf_write_page(table_id, root_pagenum, root);

    free(header_page);
    free(root);

    return 0;
}


/* Master insertion function */
int insert(int table_id, int64_t key, char * value) {

    p_pnum leaf_pair;
    page_t * header_page, * leaf_page;
    pagenum_t root_pagenum, leaf_pagenum;
    leafRecord * duplicate_flag;
    leafRecord leaf_record;

    header_page = make_page();
    buf_read_page(table_id, 0, header_page);
    root_pagenum = header_page->h.root_pagenum;
    free(header_page);

    duplicate_flag = find(table_id, root_pagenum, key);

    // The current implementation ignores duplicates
    if (duplicate_flag != NULL) {
        free(duplicate_flag);
        return 2;
    }

    free(duplicate_flag);

    // Create a new record for the value
    leaf_record = make_leaf_record(key, value);

    /* Case : the tree does not exist yet */

    if (root_pagenum == 0)
        return start_new_tree(table_id, key, leaf_record);

    leaf_pair = find_leaf_page(table_id, root_pagenum, key);
    leaf_page = leaf_pair.first;

    /* Case : leaf has room for record */

    if (leaf_page->p.num_keys < leaf_order - 1)
        insert_into_leaf(table_id, leaf_pair, key, leaf_record);

        /* Case : leaf has no room for record */

    else
        insert_into_leaf_after_splitting(table_id, leaf_pair, key, leaf_record);

    return 0;
}


/* Deletion */

page_t * remove_entry_from_page(int table_id, p_pnum page_pair, int key_index) {

    page_t * page;
    pagenum_t pagenum;
    int i, num_pointers;

    page = page_pair.first;
    pagenum = page_pair.second;

    if (page->p.is_leaf) {
        for (i = key_index + 1; i < page->p.num_keys; i++)
            page->p.l_records[i - 1] = page->p.l_records[i];
    }

    else {
        for (i = key_index + 1; i < page->p.num_keys; i++)
            page->p.i_records[i - 1] = page->p.i_records[i];
    }

    page->p.num_keys--;
    buf_write_page(table_id, pagenum, page);

    return page;
}


int adjust_root(int table_id, p_pnum root_pair) {

    page_t * header_page, * root_page, * new_root_page;
    pagenum_t root_pagenum;

    root_page = root_pair.first;
    root_pagenum = root_pair.second;

    /* Case: nonempty root */

    if (root_page->p.num_keys > 0)
        return 0;

    /* Case: empty root */

    header_page = make_page();
    buf_read_page(table_id, 0, header_page);

    // If it has a child, promote the first (only) child as the new root.
    if (!root_page->p.is_leaf) {
        new_root_page = make_page();

        buf_read_page(table_id, root_page->p.one_more_pagenum, new_root_page);
        header_page->h.root_pagenum = root_page->p.one_more_pagenum;
        new_root_page->p.parent_pagenum = 0;

        buf_write_page(table_id, header_page->h.root_pagenum, new_root_page);
        free(new_root_page);
    }

        // If it is a leaf, then the whole tree is empty.
    else
        header_page->h.root_pagenum = 0;

    buf_write_page(table_id, 0, header_page);

    buf_free_page(table_id, root_pagenum);

    free(header_page);
    free(root_page);

    return 0;
}


// Always key_page's num_keys + neighbor's num_keys < (internal or leaf)order - 1
// This means one_more_pagenum is not used when coalesce
int coalesce_nodes(int table_id, page_t * parent, p_pnum key_pair, p_pnum neighbor_pair, int neighbor_index, int k_prime) {

    page_t * key_page, * neighbor, * temp_page;
    pagenum_t parent_pagenum, key_pagenum, neighbor_pagenum;
    int i, j, neighbor_insertion_index, key_index;

    if (neighbor_index == -2) {
        key_page = neighbor_pair.first;
        key_pagenum = neighbor_pair.second;

        neighbor = key_pair.first;
        neighbor_pagenum = key_pair.second;
    }

    else {
        key_page = key_pair.first;
        key_pagenum = key_pair.second;

        neighbor = neighbor_pair.first;
        neighbor_pagenum = neighbor_pair.second;
    }

    buf_free_page(table_id, key_pagenum);

    parent_pagenum = neighbor->p.parent_pagenum;

    neighbor_insertion_index = neighbor->p.num_keys;

    /* Case : internal page */

    if (!key_page->p.is_leaf) {
        temp_page = make_page();

        neighbor->p.i_records[neighbor_insertion_index].key = k_prime;
        neighbor->p.num_keys++;

        neighbor->p.i_records[neighbor_insertion_index].pagenum = key_page->p.one_more_pagenum;

        buf_read_page(table_id, neighbor->p.i_records[neighbor_insertion_index].pagenum, temp_page);
        temp_page->p.parent_pagenum = neighbor_pagenum;
        buf_write_page(table_id, neighbor->p.i_records[neighbor_insertion_index].pagenum, temp_page);

        if (neighbor_index == -2) {
            for (i = neighbor_insertion_index + 1, j = 0; j < key_page->p.num_keys; i++, j++) {
                neighbor->p.i_records[i] = key_page->p.i_records[j];
                neighbor->p.num_keys++;

                buf_read_page(table_id, neighbor->p.i_records[i].pagenum, temp_page);
                temp_page->p.parent_pagenum = neighbor_pagenum;
                buf_write_page(table_id, neighbor->p.i_records[i].pagenum, temp_page);
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

    buf_write_page(table_id, neighbor_pagenum, neighbor);

    free(key_page);
    free(neighbor);

    delete_entry(table_id, make_pair(parent, parent_pagenum), key_index);

    return 0;
}


int redistribute_nodes(int table_id, page_t * parent, p_pnum key_pair, p_pnum neighbor_pair, int neighbor_flag
        , int k_prime_index, int k_prime) {

    // Only internal page can reach this function

    page_t * key_page, * neighbor, * temp_page;
    pagenum_t key_pagenum, neighbor_pagenum;
    int i, j;
    int num_neighbor_keys;
    int move_start_index;
    int move_cnt;

    key_page = key_pair.first;
    key_pagenum = key_pair.second;

    neighbor = neighbor_pair.first;
    neighbor_pagenum = neighbor_pair.second;

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
        buf_read_page(table_id, key_page->p.one_more_pagenum, temp_page);
        temp_page->p.parent_pagenum = key_pagenum;
        buf_write_page(table_id, key_page->p.one_more_pagenum, temp_page);

        for (i = 0; i < key_page->p.num_keys; i++) {
            buf_read_page(table_id, key_page->p.i_records[i].pagenum, temp_page);
            temp_page->p.parent_pagenum = key_pagenum;
            buf_write_page(table_id, key_page->p.i_records[i].pagenum, temp_page);
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
            buf_read_page(table_id, key_page->p.i_records[i].pagenum, temp_page);
            temp_page->p.parent_pagenum = key_pagenum;
            buf_write_page(table_id, key_page->p.i_records[i].pagenum, temp_page);
        }

        free(temp_page);
    }

    buf_write_page(table_id, key_page->p.parent_pagenum, parent);
    buf_write_page(table_id, key_pagenum, key_page);
    buf_write_page(table_id, neighbor_pagenum, neighbor);

    free(parent);
    free(key_page);
    free(neighbor);

    return 0;
}


int get_neighbor_index(int table_id, page_t * parent, pagenum_t key_pagenum) {

    int i, neighbor_index;

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


int delete_entry(int table_id, p_pnum key_pair, int key_index) {

    p_pnum neighbor_pair;
    page_t * parent, * key_page, * neighbor;
    pagenum_t key_pagenum, neighbor_pagenum;
    int neighbor_index, neighbor_flag;
    int k_prime_index, k_prime;

    key_page = key_pair.first;
    key_pagenum = key_pair.second;

    // Remove key and pointer from node.
    key_page = remove_entry_from_page(table_id, key_pair, key_index);

    /* Case : deletion from the root */

    if (key_page->p.parent_pagenum == 0)
        return adjust_root(table_id, key_pair);

    /* Case : Delayed merge  */

    // If page has a key, do nothing
    if (key_page->p.num_keys != 0) {
        free(key_page);
        return 0;
    }

    // get_neighbor index function

    parent = make_page();
    buf_read_page(table_id, key_page->p.parent_pagenum, parent);

    key_index = get_neighbor_index(table_id, parent, key_pagenum);

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
    buf_read_page(table_id, neighbor_pagenum, neighbor);

    // k_prime_index is 0 when left sibling is not exist
    // k_prime_index is neighbor_index when neighbor is exist
    k_prime_index = neighbor_flag == -2 ? 0 : neighbor_flag + 1;

    // k_prime is key value between
    // neighbor page pointer and key_page pointer in parent page
    k_prime = parent->p.i_records[k_prime_index].key;

    neighbor_pair = make_pair(neighbor, neighbor_pagenum);

    /* Case : key_page is leaf page */

    if (key_page->p.is_leaf)
        return coalesce_nodes(table_id, parent, key_pair, neighbor_pair, neighbor_flag, k_prime);

        /* Case : key_page is internal page */

    else {
        if (neighbor->p.num_keys == internal_order - 1)
            return redistribute_nodes(table_id, parent, key_pair, neighbor_pair, neighbor_flag, k_prime_index, k_prime);

        else
            return coalesce_nodes(table_id, parent, key_pair, neighbor_pair, neighbor_flag, k_prime);
    }
}


/* Master deletion function */
int _delete(int table_id, int64_t key) {

    p_pnum key_leaf_pair;
    page_t * header_page, * key_leaf_page;
    pagenum_t root_pagenum, key_leaf_pagenum;
    leafRecord * key_record;
    int key_index;

    header_page = make_page();
    buf_read_page(table_id, 0, header_page);
    root_pagenum = header_page->h.root_pagenum;
    free(header_page);

    key_record = find(table_id, root_pagenum, key);

    if (key_record == NULL) {
        free(key_record);
        return 2;
    }

    key_leaf_pair = find_leaf_page(table_id, root_pagenum, key);

    key_leaf_page = key_leaf_pair.first;
    key_leaf_pagenum = key_leaf_pair.second;

    if (key_record != NULL && key_leaf_page != NULL) {
        free(key_record);
        for (int i = 0; i < key_leaf_page->p.num_keys; i++) {
            if (key_leaf_page->p.l_records[i].key == key) {
                key_index = i;
                break;
            }
        }
    }

    return delete_entry(table_id, key_leaf_pair, key_index);;
}


/* Etc */

int cut( int length ) {
    if (length % 2 == 0)
        return length / 2;
    else
        return length / 2 + 1;
}

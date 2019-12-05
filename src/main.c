#include <stdio.h>

#include <mysql/my_global.h>
#include <mysql/mysql.h>


#define MAX_LINELENGTH 32
#define MAX_QUERYLENGTH 512


typedef struct script_params_s {
    char db_name[64];
    char db_user[64];
    char db_pswd[64];
    char table_prefix[64];
} script_params_t;

/**
 * Read the script config from a given file.
 * 
 * Config file should have the following structure:
 * ```DB_NAME DB_USER DB_PSWD TABLE_PREFIX```
 * with values without '/`/" separated by a single space.
 * 
 * @param params[out] Structure to read config into.
 * @param path[in] Path to the config file.
 * 
 * @returns On success, a positive value; on error, a negative value.
 */
int read_config(script_params_t *params, const char *path) {
    FILE *fp = fopen(path, "r");
    if (!fp)
        return -1;

    int rc = fscanf(fp, "%63s %63s %63s %63s",
                params->db_name, params->db_user, params->db_pswd, params->table_prefix);

    fclose(fp);

    if (rc == EOF)
        return -2;
    
    if (rc != 4)
        return -3;

    return 1;
}


typedef struct inputf_iter_s {
    unsigned short open;
    FILE *fp;

    int line;
    char buf[MAX_LINELENGTH];

    char ean13[14];
    int quantity;
} inputf_iter_t;

/**
 * Initialize an input iterator, opening the given file.
 * 
 * @param iter[in,out] An input iterator instance.
 * @param path[in] Path to input file.
 * @returns If opened opened successfully, returns file pointer and sets `.open` to 1.
 *          If failed to open, returns null pointer and sets `.open` to zero.
 */
void *inputf_open(inputf_iter_t *iter, const char *path) {
    iter->fp = fopen(path, "r");

    if (!iter->fp) {
        iter->open = 0;
        return NULL;
    }

    iter->line = 0;
    iter->open = 1;
    return iter->fp;
}

/**
 * Close the iterator's underlying file pointer if opened and set `.open` to zero.
 * @param iter[in,out] An input iterator instance.
 */
void inputf_close(inputf_iter_t *iter) {
    if (iter->open == 1)
        fclose(iter->fp);

    iter->open = 0;
}

/**
 * Read next entry of the input file and advance one line down.
 * 
 * @param iter[in,out] An input iterator instance.
 * @returns If read successfully 1, if reached end-of-file 0, on error -1.
 */
int inputf_read_next(inputf_iter_t *iter) {

    memset(iter->ean13, 0, 14);
    memset(iter->buf, 0, MAX_LINELENGTH);
    iter->line++;
    iter->quantity = -1;

    int scanRes;

    if (fgets(iter->buf, MAX_LINELENGTH, iter->fp) == NULL) {
        if (feof(iter->fp))
            return 0;
        return -1;
    }

    scanRes = sscanf(iter->buf, "%13s;%7d", iter->ean13, &iter->quantity);

    if (scanRes == EOF)
        return 0;

    if (scanRes != 2)
        return -1;

    return 1;
}


/**
 * Program entry point.
 * Usage: ./my_import_quantities input_file db_config_file
 */
int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Incorrect number of arguments provided to the script\n");
        goto error_exit;
    }

    fprintf(stdout, "MySQL client ver: %s\n", mysql_get_client_info());
    fprintf(stdout, "Script set to read from '%s' and load config from '%s'\n", argv[1], argv[2]);


    // Parse configuration

    script_params_t config = {};
    int rc = read_config(&config, argv[2]);
    if (rc < 1) {
        fprintf(stderr, "Failed to read config from %s (%d)\n", argv[2], rc);
        goto error_exit;
    }

    fprintf(stdout, "Config parsed, connecting to '%s'@'localhost', prefix='%s' as '%s'\n",
        config.db_name, config.table_prefix, config.db_user);


    // Initialize the database connection
    
    MYSQL *conn = mysql_init(NULL);
    if (conn == NULL) {
        fprintf(stderr, "Failed to initialize MYSQL: %s\n", mysql_error(conn));
        goto error_exit;
    }

    if (NULL == mysql_real_connect(conn, "localhost", config.db_user, config.db_pswd, config.db_name, 0, NULL, 0)) {
        fprintf(stderr, "Failed to connect to the database: %s\n", mysql_error(conn));
        goto cleanup_error_exit;
    }

    fprintf(stdout, "Connected to the database\n");


    // Parse the input file

    inputf_iter_t inputIter = {};
    if (inputf_open(&inputIter, argv[1]) == NULL) {
        fprintf(stderr, "Failed to open input file '%s'\n", argv[1]);
        goto cleanup_error_exit;
    }

    fprintf(stdout, "Opened input file\n");


    // Start the transaction

    if (mysql_autocommit(conn, FALSE) != 0) {
        fprintf(stderr, "Failed to disable autocommit: %s\n", mysql_error(conn));
        inputf_close(&inputIter);
        goto cleanup_error_exit;
    }


    // Prepare the statements

    char prefixedSqlProductUpdateQuery[MAX_QUERYLENGTH];
    char prefixedSqlIdStockAvailableQuery[MAX_QUERYLENGTH];

    if (snprintf(prefixedSqlProductUpdateQuery, MAX_QUERYLENGTH,
            "UPDATE %sproduct SET quantity=? WHERE ean13=?;", config.table_prefix) < 0) {
        fprintf(stderr, "Failed to set prefix for product update statement\n");
        inputf_close(&inputIter);
        goto cleanup_error_exit;
    }

    if (snprintf(prefixedSqlIdStockAvailableQuery, MAX_QUERYLENGTH,
            "SELECT id_stock_available" \
            " FROM %sproduct_attribute" \
            " INNER JOIN %sstock_available" \
            "   ON %sproduct_attribute.id_product=%sstock_available.id_product" \
            "     AND %sproduct_attribute.id_product_attribute=%sstock_available.id_product_attribute" \
            " WHERE ean13=?;",
            config.table_prefix, config.table_prefix, config.table_prefix,
                config.table_prefix, config.table_prefix, config.table_prefix) < 0) {
        fprintf(stderr, "Failed to set prefix for `id stock availabile` statement\n");
        inputf_close(&inputIter);
        goto cleanup_error_exit;
    }

    MYSQL_STMT *stmtUpdateProduct = mysql_stmt_init(conn);
    if (0 != mysql_stmt_prepare(stmtUpdateProduct, 
                prefixedSqlProductUpdateQuery, strlen(prefixedSqlProductUpdateQuery))) {
        fprintf(stderr, "Failed to prepare product update statement: %s\n", mysql_stmt_error(stmtUpdateProduct));
        inputf_close(&inputIter);
        goto cleanup_error_exit;
    }

    MYSQL_STMT *stmtIdStockAvailable = mysql_stmt_init(conn);
    if (0 != mysql_stmt_prepare(stmtIdStockAvailable, 
                prefixedSqlIdStockAvailableQuery, strlen(prefixedSqlIdStockAvailableQuery))) {
        fprintf(stderr, "Failed to prepare `id stock availabile` statement: %s\n", mysql_stmt_error(stmtIdStockAvailable));
        
        if (mysql_stmt_close(stmtUpdateProduct) != 0)
            fprintf(stderr, "Failed to close prepared product update statement: %s\n", mysql_error(conn));
        
        inputf_close(&inputIter);
        goto cleanup_error_exit;
    }

    int loopProductQuantity = -1;
    char loopEan13[14];

    MYSQL_BIND bindUpdateProduct[2];
    memset(bindUpdateProduct, 0, sizeof(bindUpdateProduct));
    bindUpdateProduct[0].buffer_type = MYSQL_TYPE_LONG;
    bindUpdateProduct[0].buffer = (int *)&loopProductQuantity;
    bindUpdateProduct[0].length = 0;
    bindUpdateProduct[1].buffer_type = MYSQL_TYPE_STRING;
    bindUpdateProduct[1].buffer = &loopEan13;
    long unsigned int loopEan13Length = 14;
    bindUpdateProduct[1].buffer_length = loopEan13Length;
    bindUpdateProduct[1].length = &loopEan13Length;

    if (0 != mysql_stmt_bind_param(stmtUpdateProduct, bindUpdateProduct)) {
        fprintf(stderr, "Failed to bind params for the product update statement: %s\n", mysql_stmt_error(stmtUpdateProduct));

        if (mysql_stmt_close(stmtUpdateProduct) != 0)
            fprintf(stderr, "Failed to close prepared product update statement: %s\n", mysql_error(conn));

        if (mysql_stmt_close(stmtIdStockAvailable) != 0)
            fprintf(stderr, "Failed to close `id stock availabile` update statement: %s\n", mysql_error(conn));

        inputf_close(&inputIter);
        goto cleanup_error_exit;
    }

    while ((rc = inputf_read_next(&inputIter)) > 0) {
        // 2. regardless of the result, get the combination id with EAN13.
        // 3. if such a combination exists, update its value.
        // 4. if neither combination not product exis, log a warning.


        // 1. try to set product's quantity if it has EAN13

        long affected_rows = 0;
        long affected_rows_step1 = 0;

        loopProductQuantity = inputIter.quantity;
        strncpy(loopEan13, inputIter.ean13, 13);

        if (0 != mysql_stmt_execute(stmtUpdateProduct)) {
            fprintf(stderr, "Failed to update a product with EAN13=%s (line %d)\n", inputIter.ean13, inputIter.line);
        }
        
        if ((affected_rows_step1 = (long)mysql_stmt_affected_rows(stmtUpdateProduct)) > 0)
            affected_rows += affected_rows_step1;
        
    }

    if (rc != 0) {
        fprintf(stderr, "Invalid input file at line %d, rolling back\n", inputIter.line);

        if (mysql_rollback(conn) != 0)
            fprintf(stderr, "Failed to roll back the transaction: %s\n", mysql_error(conn));
        
        if (mysql_stmt_close(stmtUpdateProduct) != 0)
            fprintf(stderr, "Failed to close prepared product update statement: %s\n", mysql_error(conn));

        if (mysql_stmt_close(stmtIdStockAvailable) != 0)
            fprintf(stderr, "Failed to close prepared `id stock availabile` statement: %s\n", mysql_error(conn));

        inputf_close(&inputIter);
        goto cleanup_error_exit;
    }


    // Commit the transaction

    if (mysql_commit(conn) != 0)
        fprintf(stderr, "Failed to commit the transaction: %s\n", mysql_error(conn));
    else
        fprintf(stdout, "Committed the transaction\n");


    // Cleanup & exit points
    
    goto cleanup;

cleanup:
    if (mysql_stmt_close(stmtUpdateProduct) != 0 ||
            mysql_stmt_close(stmtUpdateProduct) != 0) {
        fprintf(stderr, "Failed to close prepared one or more prepared statements: %s\n", mysql_error(conn));
        inputf_close(&inputIter);
        goto cleanup_error_exit;
    }
    
    inputf_close(&inputIter);
    goto good_exit;

good_exit:
    fprintf(stdout, "Work done, program exiting\n");
    mysql_close(conn);
    return 0;

cleanup_error_exit:
    fprintf(stderr, "Closing database connection\n");
    mysql_close(conn);

error_exit:
    return 1;
}

#include <stdio.h>

#include <mysql/my_global.h>
#include <mysql/mysql.h>


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

    fprintf(stdout, "Config parsed, connecting to '%s' (prefix='%s') as '%s'\n",
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


    // Cleanup & exit points.
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

/* 
 * File:   roma_client.h
 * Author: yosuke
 *
 * Created on 2009/10/20
 */

#ifndef _ROMA_CLIENT_PRIVATE_H
#define	_ROMA_CLIENT_PRIVATE_H

#ifdef	__cplusplus
extern "C" {
#endif

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

// ========================================================
//  
// ========================================================
#define RMC_SERVER_ERROR   -1
#define RMC_NOT_STORED     -2
#define RMC_NOT_FOUND      -3
#define RMC_NOT_CLEARED    -4
#define RMC_ALIST_NULL     -5

#define RMC_STORED          1
#define RMC_DELETED         2
#define RMC_CLEARED         3

#define RMC_ALIST_TRUE      5
#define RMC_ALIST_FALSE     6

#define RMC_TIMEOUT         3

/* roma-client commands. */
enum rmc_command {
    RMC_SET     = 0,
    RMC_ADD     = 1,
    RMC_REPLACE = 2,
    RMC_APPEND  = 3,
    RMC_PREPEND = 4,
    RMC_CAS     = 5,
    RMC_DELETE  = 6,
    RMC_GET     = 7
};

/**
 * value info.
 */
typedef struct {
    char *value;
    int  length;
    int  error;
    char *cause;
} rmc_value_info;

/**
 * roma-host info.
 */
typedef struct {
    char *ip_address;
    int port;
    int connection;
} rmc_host_info;

/**
 * routing-data.
 */
typedef struct {
    unsigned int count;
    char *node;
} rmc_fail_count;

typedef struct {
    char *node;
} rmc_node;

// virtual-node.
typedef struct {
    unsigned long index;
    rmc_node *nodes;
} rmc_virtual_node;

// routing-data.
typedef struct {
    unsigned int dgst_bits;
    unsigned int div_bits;
    unsigned int rn;
    unsigned long search_mask;
    unsigned int number_of_nodes;
    rmc_node *nodes;
    rmc_virtual_node *v_idx;

    unsigned long long hbits;
    char *mklhash;
    rmc_fail_count *fail_count;
} rmc_routing_data;

typedef struct {
    char *node;
    unsigned int digest;
} rmc_digest_node;

// ========================================================
// roma connection related.
// ========================================================
/**
 * connect roma-server.
 * @param[in] hosts of number.
 * @param[in] roma-hosts.
 * @return connection
 */
int connect_roma_server(const int hosts, const char **str_romahosts);

/**
 * disconnect roma-server.
 * @return success/faiure
 */
int disconnect_roma_server();

// ========================================================
// node management related.
// ========================================================
/**
 * select a alive node in rmc_romahosts structure by the random method.
 * @return address and portnumber string (must be free after using)
 */
  char * rmc_select_node_by_rand();

// ========================================================
// routing-table related.
// ========================================================
/**
 * roma-client - create routing table.
 * @param[in] rd - routing data
 * @param[in] nodes
 * @return [OK|NG]
 */
int rmc_create_routing_table(rmc_routing_data *rd);

/**
 * roma-client - check routing data as yaml.
 * @param[in] routing dump as yaml
 * @return [OK|NG]
 */
int rmc_check_routing_data(char *routing_dump_yaml);

/**
 * roma-client - generate routing data.
 * @param[in]  routing dump as yaml
 * @param[in]  number of nodes.
 * @return routing-data
 */
rmc_routing_data rmc_generate_routing_data(const char *routing_dump_yaml, const int nodes);

/**
 * roma-client - search node.
 * @param key
 * @param rd - routing data
 * @return digest and node.
 *
 */
rmc_digest_node rmc_search_node(const char *key, rmc_routing_data rd);

// ========================================================
// command-sender related.
// ========================================================
/**
 * roma-client - send route mklhash.
 * @param[in]  connection
 * @return mklhash
 */
char *rmc_send_route_mklhash(const int connection);

/**
 * send a set command.
 *
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @param[in] valinfo
 * @param[in] exptime
 * @return [OK|NG]
 */
int rmc_send_set(
    const int connection, const char *key, const unsigned int digest,
    const rmc_value_info valinfo, const int exptime);

/**
 * send a add command.
 *
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @param[in] valinfo
 * @param[in] exptime
 * @return [OK|NG]
 */
int rmc_send_add(
    const int connection, const char *key, const unsigned int digest,
    const rmc_value_info valinfo, const int exptime);

/**
 * send a add replace.
 *
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @param[in] value
 * @param[in] exptime
 * @return [OK|NG]
 */
int rmc_send_replace(
    const int connection, const char *key, const unsigned int digest,
    const rmc_value_info valinfo, const int exptime);

/**
 * send a append command.
 *
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @param[in] valinfo
 * @param[in] exptime
 * @return [OK|NG]
 */
int rmc_send_append(
    const int connection, const char *key, const unsigned int digest,
    const rmc_value_info valinfo, const int exptime);

/**
 * send a prepend command.
 *
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @param[in] valinfo
 * @param[in] exptime
 * @return [OK|NG]
 */
int rmc_send_prepend(
    const int connection, const char *key, const unsigned int digest,
    const rmc_value_info valinfo, const int exptime);

/**
 * send a delete command.
 *
 * @param[in] connection
 * @param[in] key
 * @return
 */
int rmc_send_delete(const int connection, const char *key);

/**
 * send a get command.
 *
 * @param[in] connection
 * @param[in] key
 * @return value
 */
rmc_value_info rmc_send_get(const int connection, const char *key);

/**
 * roma-client - send version.
 * @param[in]  connection
 * @return version
 */
char *rmc_send_version(const int connection);

/**
 * roma-client - send quit.
 * @param[in]  connection
 * @return [OK|NG]
 */
int rmc_send_quit(const int connection);

/**
 * roma-client - send routedump as yaml.
 * @param[in]  connection
 * @param[in]  number of nodes
 * @return routing-data
 */
rmc_routing_data rmc_send_routedump_as_yaml(const int connection, const int nodes);

/**
 * send alist at command.
 * @param connection
 * @param key
 * @param index
 * @return value
 */
rmc_value_info rmc_send_alist_at(const int connection, const char *key, const int index);

/**
 * send alist clear command.
 * @param connection
 * @param key
 * @return status
 */
int rmc_send_alist_clear(const int connection, const char *key);

/**
 * send alist delete.
 * @param connection
 * @param key
 * @param valinfo
 * @return status
 */
int rmc_send_alist_delete(const int connection, const char *key, const rmc_value_info valinfo);

/**
 * send alist delete at command.
 * @param connection
 * @param key
 * @param index
 * @return status
 */
int rmc_send_alist_delete_at(const int connection, const char *key, const int index);

/**
 * send alist empty command.
 * @param connection
 * @param key
 * @return status
 */
int rmc_send_alist_empty(const int connection, const char *key);

/**
 * send alist first command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_first(const int connection, const char *key);

/**
 * send alist include command.
 * @param connection
 * @param key
 * @param value
 * @return status
 */
int rmc_send_alist_include(
    const int connection, const char *key, const rmc_value_info valinfo);

/**
 * send alist index command.
 * @param connection
 * @param key
 * @param value
 * @return index
 */
int rmc_send_alist_index(const int connection, const char *key, const rmc_value_info valinfo);

/**
 * plugin - alist insert.
 * @param connection
 * @param key
 * @param index
 * @param valinfo
 * @return [OK|NG]
 */
int rmc_send_alist_insert(
    const int connection, const char *key, const int index, const rmc_value_info valinfo);

/**
 * send alust sized insert command.
 * @param connection
 * @param key
 * @param size
 * @param valinfo
 * @return status
 */
int rmc_send_alist_sized_insert(
    const int connection, const char *key, const int size, const rmc_value_info valinfo);

/**
 * plugin - alist join
 * @param connection
 * @param key
 * @param separator
 * @return value
 */
rmc_value_info rmc_send_alist_join(const int connection, const char *key, const char *separator);

/**
 * send alist to json command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_to_json(const int connection, const char *key);

/**
 * send alist last command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_last(const int connection, const char *key);

/**
 * send alist length command.
 * @param connection
 * @param key
 * @return status
 */
int rmc_send_alist_length(const int connection, const char *key);

/**
 * send alist pop command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_pop(const int connection, const char *key);

/**
 * send alist push command.
 * @param connection
 * @param key
 * @param value
 * @return status
 */
int rmc_send_alist_push(const int connection, const char *key, const rmc_value_info valinfo);

/**
 * send alist shift command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_shift(const int connection, const char *key);

/**
 * plugin - alist to string.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_tostr(const int connection, const char *key);

#ifdef	__cplusplus
}
#endif

#endif	/* _ROMA_CLIENT_PRIVATE_H */

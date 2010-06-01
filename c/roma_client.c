/* 
 * File:   rmc_client.c
 * Author: yosuke
 *
 * Created on 2009/06/26
 */

#include <stdio.h>
#include <stdlib.h>
#include "roma_client_private.h"

#define RMC_COMMAND_TMP "send_%s_command"

static char *_daemon_host;
rmc_routing_data _rd;
char *RMC_COMMAND_STR[8] =
    {"set", "add", "replace", "append", "prepend", "cas", "delete", "get"};


/**
 * make routing table.
 *
 * @param[in] node
 * @return
 */
static int _rmc_create_routing_table(const int number_of_nodes, const char **nodes)
{
    int i, conn=0;
    for (i = 0; i<number_of_nodes; i++)
    {        
        conn = rmc_get_connection(nodes[i]);
        if (conn > 0)
            break;
    }
    if (conn == 0) return (EXIT_FAILURE);

    _rd = rmc_send_routedump_as_yaml(conn, number_of_nodes); //@TODO!
    if (_rd.dgst_bits == 0 || _rd.div_bits == 0)
    {
        return (EXIT_FAILURE);
    }
    _rd.number_of_nodes = number_of_nodes;
    rmc_create_routing_table(&_rd);
 
    return (EXIT_SUCCESS);
}

/**
 * connect.
 * @param[in] hosts - number of hosts.
 * @param[in] str_hosts (ip-addresses).
 */
int rmc_connect(const int hosts, const char** str_hosts)
{
    int ret = 0;
    ret = connect_roma_server(hosts, str_hosts);
    if(ret == EXIT_FAILURE)
    {
        return (ret);
    }
    
    if (hosts == 1)
    {
        _daemon_host = (char *)calloc(strlen(str_hosts[0])+1, sizeof(char));
        strcpy(_daemon_host, str_hosts[0]);
        return (EXIT_SUCCESS);
    }
    else {
        _daemon_host = NULL;
        ret = _rmc_create_routing_table(hosts, str_hosts);
        return (EXIT_SUCCESS);
    }
}

/**
 * disconnect.
 *
 */
int rmc_disconnect()
{  
   if (_daemon_host == NULL)
   {
        free(_rd.nodes);
        free(_rd.v_idx);
   } else {
        free(_daemon_host);
   }
    disconnect_roma_server();
}

/**
 * get coonection.
 * @param key
 * @param
 * @return connection
 */
static int _rmc_get_connection(const char *key) {
    int con = 0;
    if (_daemon_host == NULL)
    {
        rmc_digest_node dn = rmc_search_node(key, _rd);
        con = rmc_get_connection(dn.node);

        if (con < 1) {
            int i;
            for (i =  0; i < _rd.number_of_nodes; i++) {
                if (!strcmp(dn.node, _rd.nodes[i].node))
                {
                    //printf("***next:node:[%s]\n", _rd.nodes[i].node);
                    con = rmc_get_connection(_rd.nodes[i].node);
                    if (con > 0) break;
                }
            }
        }
    }
    else {
        con = rmc_get_connection(_daemon_host);
    }
    return (con);
}

/**
 * update routing table.
 *
 * @param[in] nodes
 * @return 
 */
static int _rmc_update_routing_table(char **nodes)
{
    if (nodes == NULL || sizeof nodes == 0) {
        return (EXIT_FAILURE);
    }
    return (EXIT_SUCCESS);
}

/**
 * send set command.
 * @param command
 * @param key
 * @param valinfo
 * @param exptime
 * @return status
 */
static int _rmc_send_set_command(
    const int command, const char *key, rmc_value_info valinfo, const int exptime)
{
    int con = _rmc_get_connection(key);
    char str_command[128];
    sprintf(str_command, RMC_COMMAND_TMP, RMC_COMMAND_STR[command]);

    int ret = 0;
    int digest = 0;
    switch (command)
    {
        case RMC_SET:
            ret = rmc_send_set(con, key, digest, valinfo, exptime);
            break;
        case RMC_ADD:
            ret = rmc_send_add(con, key, digest, valinfo, exptime);
            break;
        case RMC_REPLACE:
            ret = rmc_send_replace(con, key, digest, valinfo, exptime);
            break;
        case RMC_APPEND:
            ret = rmc_send_append(con, key, digest, valinfo, exptime);
            break;
        case RMC_PREPEND:
            ret = rmc_send_prepend(con, key, digest, valinfo, exptime);
            break;
        case RMC_CAS: //@pending...
            break;
        case RMC_DELETE:
            ret = rmc_send_delete(con, key);
            break;
        default:
            break;
    }
    return (ret);
}


/**
 * update_data
 * @param[in] command
 * @param[in] key
 * @param[in] value
 * @param[in] exptime
 * @return value
 */
static rmc_value_info _rmc_get_command(const int command, const char *key)
{
    char str_command[128];
    sprintf(str_command, RMC_COMMAND_TMP, RMC_COMMAND_STR[command]);

    int con = _rmc_get_connection(key);
    rmc_value_info ret_value = rmc_send_get(con, key);
    return (ret_value);
}

/**
 * set command.
 * @param[in] key
 * @param[in] valinfo [value|length]
 * @param[in] exptime
 * @return
 */

int rmc_set(const char *key, const rmc_value_info valinfo, const int exptime)
{
    int ret = _rmc_send_set_command(RMC_SET, key, valinfo, exptime);
    return (ret);
}

/**
 * add command.
 * @param[in] key
 * @param[in] valinfo
 * @param[in] exptime
 * @return
 */
int rmc_add(const char *key, const rmc_value_info valinfo, const int exptime)
{
    int ret = _rmc_send_set_command(RMC_ADD, key, valinfo, exptime);
    return (ret);
}

/**
 * replace command.
 * @param[in] key
 * @param[in] value
 * @param[in] exptime
 * @return
 */
int rmc_replace(const char *key, const rmc_value_info valinfo, const int exptime)
{
    int ret = _rmc_send_set_command(RMC_REPLACE, key, valinfo, exptime);
    return (ret);
}

/**
 * append command.
 * @param[in] key
 * @param[in] value
 * @param[in] exptime
 * @return 
 */
int rmc_append(const char *key, const rmc_value_info valinfo, const int exptime)
{
    int ret = _rmc_send_set_command(RMC_APPEND, key, valinfo, exptime);
    return (ret);
}

/**
 * prepend command.
 * @param[in] key
 * @param[in] value
 * @param[in] exptime
 * @return
 */
int rmc_prepend(const char *key, const rmc_value_info valinfo, const int exptime)
{
    int ret = _rmc_send_set_command(RMC_PREPEND, key, valinfo, exptime);
    return (ret);
}

/**
 * cas command.
 * @param[in] key
 * @param[in] value
 * @param[in] exptime
 * @return
 */
int rmc_cas(const char *key, const rmc_value_info valinfo, const int exptime)
{
    int ret = _rmc_send_set_command(RMC_CAS, key, valinfo, exptime);
    return (ret);
}

/**
 * delete command.
 * @param[in] key
 * @param[in] value
 * @param[in] exptime
 * @return
 */
int rmc_delete(const char *key)
{
    rmc_value_info valinfo;
    int ret = _rmc_send_set_command(RMC_DELETE, key, valinfo, 0);
    return (ret);
}

/**
 * get command.
 * @param[in] key
 * @return 
 */
rmc_value_info rmc_get(const char *key) {
    rmc_value_info ret = _rmc_get_command(RMC_GET, key);
    return ret;
}

/**
 * alist at command.
 * @param key
 * @param index
 * @return value
 */
rmc_value_info rmc_alist_at(const char *key, const int index)
{
    int con = _rmc_get_connection(key);
    rmc_value_info ret = rmc_send_alist_at(con, key, index);
    return (ret);
}

/**
 * alist clear command.
 * @param key
 * @return status
 */
int rmc_alist_clear(const char *key)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_clear(con, key);
    return (ret);
}

/**
 * alist delete command.
 * @param key
 * @return status
 */
int rmc_alist_delete(const const char *key, const rmc_value_info valinfo)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_delete(con, key, valinfo);
    return (ret);
}

/**
 * alist delete at command.
 * @param key
 * @param index
 * @return status
 */
int rmc_alist_delete_at(const char *key, const int index)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_delete_at(con, key, index);
    return (ret);
}

/**
 * alist empty command.
 * @param key
 * @return status
 */
int rmc_alist_empty(const char *key)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_empty(con, key);
    return (ret);
}

/**
 * alist fist command.
 * @param key
 * @return value
 */
rmc_value_info rmc_alist_first(const char *key)
{
    int con = _rmc_get_connection(key);
    rmc_value_info ret = rmc_send_alist_first(con, key);
    return (ret);
}

/**
 * alist include command.
 * @param key
 * @param valinfo
 * @return status
 */
int rmc_alist_include(const char *key, const rmc_value_info valinfo)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_include(con, key, valinfo);
    return (ret);
}

/**
 * alist index command.
 * @param key
 * @param valinfo
 * @return value
 *
 */
int rmc_alist_index(const char *key, const rmc_value_info valinfo)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_index(con, key, valinfo);
    return (ret);
}

/**
 * alist insert command.
 * @param key
 * @param index
 * @param valinfo
 * @return status
 *
 */
int rmc_alist_insert(const char *key, const int index, const rmc_value_info valinfo)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_insert(con, key, index, valinfo);
    return (ret);
}

/**
 * alist sized insert command.
 * @param key
 * @param size
 * @param valinfo
 * @return status
 *
 */
int rmc_alist_sized_insert(const char *key, const int size, const rmc_value_info valinfo)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_sized_insert(con, key, size, valinfo);
    return (ret);
}

/**
 * alist join command.
 * @param key
 * @param separator
 * @return value
 */
rmc_value_info rmc_alist_join(const char *key, const char *separator)
{
    int con = _rmc_get_connection(key);
    rmc_value_info result = rmc_send_alist_join(con, key, separator);
    return (result);
}

/**
 * alist to json command.
 * @param key
 * @return value
 */
rmc_value_info rmc_alist_to_json(const char * key)
{
    int con = _rmc_get_connection(key);
    rmc_value_info result = rmc_send_alist_to_json(con, key);
    return (result);
}

/**
 * alist last command.
 * @param key
 * @return value
 */
rmc_value_info rmc_alist_last(const char *key)
{
    int con = _rmc_get_connection(key);
    rmc_value_info ret = rmc_send_alist_last(con, key);
    return (ret);
}

/**
 * alist length command.
 * @param key
 * @return status
 */
int rmc_alist_length(const char *key)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_length(con, key);
    return (ret);
}

/**
 * alist pop command.
 * @param key
 * @return value
 */
rmc_value_info rmc_alist_pop(const char *key)
{
    int con = _rmc_get_connection(key);
    rmc_value_info ret = rmc_send_alist_pop(con, key);
    return (ret);
}

/**
 * alist push command.
 * @param key
 * @param valinfo
 * @return status
 *
 */
int rmc_alist_push(const char *key, const rmc_value_info valinfo)
{
    int con = _rmc_get_connection(key);
    int ret = rmc_send_alist_push(con, key, valinfo);
    return (ret);
}

/**
 * alist shift command.
 * @param key
 * @return value
 */
rmc_value_info rmc_alist_shift(const char *key)
{
    int con = _rmc_get_connection(key);
    rmc_value_info ret = rmc_send_alist_shift(con, key);
    return (ret);
}

/**
 * alist to string command.
 * @param key
 * @return value.
 */
rmc_value_info rmc_alist_tostr(const char *key)
{
    int con = _rmc_get_connection(key);
    rmc_value_info ret = rmc_send_alist_tostr(con, key);
    return (ret);
}

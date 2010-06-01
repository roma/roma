/*
 * File:   roma_sender.c
 * Author: yosuke hara
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "roma_client_private.h"

#define DEF_BUFSIZE_1K 1024
#define DEF_BUFSIZE_4K 4096
#define DEF_BUFSIZE_S 64

#define RMC_CMD_MKLHASH            "mklhash 0\r\n"
#define RMC_CMD_RTDUMP_YAML_BYTES  "routingdump yamlbytes\r\n"
#define RMC_CMD_RTDUMP_YAML        "routingdump yaml\r\n"
#define RMC_CMD_SET                "set %s %d %d %d\r\n"
#define RMC_CMD_ADD                "add %s %d %d %d\r\n"
#define RMC_CMD_REPLACE            "replace %s %d %d %d\r\n"
#define RMC_CMD_APPEND             "append %s %d %d %d\r\n"
#define RMC_CMD_PREPEND            "prepend %s %d %d %d\r\n"
#define RMC_CMD_DELETE             "delete %s\r\n"
#define RMC_CMD_GET                "get %s\r\n"
#define RMC_CMD_INCL               "incr %s %s\r\n"
#define RMC_CMD_DECL               "decr %s %s\r\n"
#define RMC_CMD_VERSION            "version\r\n"
#define RMC_CMD_QUIT               "quit\r\n"

#define RMC_CMD_ALIST_AT           "alist_at %s %d\r\n"
#define RMC_CMD_ALIST_CLEAR        "alist_clear %s\r\n"
#define RMC_CMD_ALIST_DELETE       "alist_delete %s %d\r\n"
#define RMC_CMD_ALIST_DELETE_AT    "alist_delete_at %s %d\r\n"
#define RMC_CMD_ALIST_EMPTY        "alist_empty? %s\r\n"
#define RMC_CMD_ALIST_FIRST        "alist_first %s\r\n"
#define RMC_CMD_ALIST_INCLUDE      "alist_include? %s %d\r\n"
#define RMC_CMD_ALIST_INDEX        "alist_index %s %d\r\n"
#define RMC_CMD_ALIST_INSERT       "alist_insert %s %d %d\r\n"
#define RMC_CMD_ALIST_SIZED_INSERT "alist_sized_insert %s %d %d\r\n"

#define RMC_CMD_ALIST_JOIN         "alist_join %s %d\r\n%s\r\n"
#define RMC_CMD_ALIST_TO_JSON      "alist_to_json %s \r\n"
#define RMC_CMD_ALIST_LAST         "alist_last %s\r\n"
#define RMC_CMD_ALIST_LENGTH       "alist_length %s\r\n"
#define RMC_CMD_ALIST_POP          "alist_pop %s\r\n"
#define RMC_CMD_ALIST_PUSH         "alist_push %s %d\r\n"
#define RMC_CMD_ALIST_SHIFT        "alist_shift %s\r\n"
#define RMC_CMD_ALIST_TO_STR       "alist_to_s %s\r\n"

#define RMC_STATUS_STORED          "STORED"
#define RMC_STATUS_NOT_STORED      "NOT_STORED"
#define RMC_STATUS_DELETED         "DELETED"
#define RMC_STATUS_NOT_FOUND       "NOT_FOUND"
#define RMC_STATUS_CLEARED         "CLEARED"
#define RMC_STATUS_NOT_CLEARED     "NOT_CLEARED"
#define RMC_STATUS_SERVER_ERROR    "SERVER_ERROR"
#define RMC_STATUS_NULL            "nil"
#define RMC_STATUS_TRUE            "true"
#define RMC_STATUS_FALSE           "false"

#define RMC_STATUS_NOT_STORED_CRLF "NOT_STORED\r\n"

#define RMC_EMPTY_VALUE            "END\r\n"
#define RMC_CRLF                   "\r\n"
#define RMC_RECV_END_VALUE         "\r\nEND\r\n"
#define RMC_CHR_CODE_LF            10
#define RMC_CHR_CODE_CR            13
#define RMC_CHR_CODE_SPACE         32
#define RMC_CRLF_SIZE               2

#define RMC_RECV_ERROR "SERVERERROR could not received data.\r\n"
#define RMC_SEND_ERROR "SERVERERROR could not sent data.\r\n"

/**
 * check receive data.
 * @param conn - connection.
 * @return [TRUE:1, FALSE:Else]
 */
static int _rmc_check_recv(int conn)
{
    fd_set fdset;
    FD_ZERO(&fdset);
    //FD_SET(0, &fdset);
    FD_SET(conn, &fdset);

    struct timeval timeout;
    timeout.tv_sec  = RMC_TIMEOUT;
    timeout.tv_usec = 0;

    int ret = select(conn+1, &fdset, NULL, NULL, &timeout);
    //int ret = select(1, &fdset, NULL, NULL, &timeout);
    return (ret);
}

/**
 * check send data.
 * @param conn - connection
 * @return [TRUE:1, FALSE:Else]
 */
static int _rmc_check_send(int conn)
{
    fd_set fdset;
    FD_ZERO(&fdset);
    //FD_SET(0, &fdset);
    FD_SET(conn, &fdset);

    struct timeval timeout;
    timeout.tv_sec  = RMC_TIMEOUT;
    timeout.tv_usec = 0;

    int ret = select(conn+1, NULL, &fdset, NULL, &timeout);
    //int ret = select(1, NULL, &fdset, NULL, &timeout);
    return (ret);
}

/**
 * send command.
 * @param connection
 * @param command
 * @param length
 * @param bufsize
 * @return result
 */
static char *_rmc_send_command(
    const int connection, const char *command, const int length, const int bufsize)
{
    char *buf = (char *)calloc(bufsize, sizeof(char));

    // check send.
    if (_rmc_check_send(connection) != 1)
    {
        fprintf(stderr, "Error in rmc_check_send() %d - %s\n", connection, strerror(connection));
        buf = RMC_SEND_ERROR;
        return (buf);
    }
    send(connection, command, length, 0);

    // check receive
    if (_rmc_check_recv(connection) != 1)
    {
        fprintf(stderr, "Error in rmc_check_recv() %d - %s\n", connection, strerror(connection));
        buf = RMC_RECV_ERROR;
        return (buf);
    }

    // receive data.
    int ret_recv, recvbytes = 0;
    while (1)
    {
        ret_recv = recv(connection, buf + recvbytes, bufsize - recvbytes, 0);
        if (ret_recv < 1)
            break;

        recvbytes += ret_recv;

        if (bufsize <= DEF_BUFSIZE_4K) break;
        if (bufsize <= recvbytes) break;
    }
    //printf("_rmc_send_command-result:[%s]\n",buf);
    return (buf);
}

/**
 * gets.
 * @param connection
 * @param buffer
 * @param length
 * @return receive_length
 */
static int _rmc_gets(int connection, char *rbuf)
{
    int recvbytes = 0, ret_recv;
    
    // check receive.
    if (_rmc_check_recv(connection) != 1)
    {
        fprintf(stderr, "Error in rmc_check_recv() %d - %s\n", connection, strerror(connection));
        rbuf = RMC_RECV_ERROR;
        recvbytes = strlen(RMC_RECV_ERROR);
        return (recvbytes);
    }

    // receive data.
    while (1)
    {
        ret_recv = recv(connection, rbuf + recvbytes, 1, 0);
        if (ret_recv < 1)
        break;

        recvbytes += ret_recv;
        if(recvbytes > 0 && rbuf[recvbytes - 1] == RMC_CHR_CODE_LF)
            break;
    }
    return recvbytes;
}

/**
 * receive.
 * @param connection
 * @pram buffer
 * @param length
 * @return receive_length
 */
static int _rmc_recv(int connection, char *rbuf, int len)
{
    int recvbytes = 0, ret_recv;
    
    // check receive.
    if (_rmc_check_recv(connection) != 1)
    {
        fprintf(stderr, "Error in rmc_check_recv() %d - %s\n", connection, strerror(connection));
        rbuf = RMC_RECV_ERROR;
        recvbytes = strlen(RMC_RECV_ERROR);
        return (recvbytes);
    }

    // receive data.
    while(recvbytes < len)
    {
        ret_recv = recv(connection, rbuf + recvbytes, len - recvbytes, 0);
        if(ret_recv < 1)
            return ret_recv;

        recvbytes += ret_recv;
    }
    return recvbytes;
}

/**
 * get body length.
 * @param value
 * @return bytes
 */
static int _rmc_get_body_length(const char *value)
{
    int bytes = 0;
    if (value != NULL && strlen(value) > strlen(RMC_EMPTY_VALUE))
    {
        int i, c = -1, startp = 0, endp = 0, length = 0;
        for (i = 0; i < strlen(value); i++)
        {
            if (c % 2 == 0 && startp > 0) {
                endp = startp + (i - startp) + 1;
                length = endp - startp -2;
                break;
            }
            if (value[i] == RMC_CHR_CODE_SPACE)
                startp = i + 1;
            if (value[i] == RMC_CHR_CODE_LF
             || value[i] == RMC_CHR_CODE_CR)
                c++;
        }
        char strbytes[length + 1];
        for (i = 0; i < length; i++)
            strbytes[i] = value[startp + i];
        strbytes[length] = '\0';
        bytes = atoi(strbytes);
    }
    return bytes;
}

/**
 * send get command.
 * @param connection
 * @param command
 * @param length
 * @param line - length
 * @return value
 */
static rmc_value_info _rmc_get_command(
    const int connection, const char *command, const int length, const int line)
{
    rmc_value_info valinfo;
    valinfo.length = 0;
    valinfo.value = NULL;
    valinfo.error = FALSE;
    valinfo.cause = NULL;

    // check send.
    if (_rmc_check_send(connection) != 1)
    {
        fprintf(stderr, "Error in rmc_check_send() %d - %s\n", connection, strerror(connection));
        valinfo.cause = RMC_SEND_ERROR;
        valinfo.error = TRUE;
        return valinfo;
    }
    send(connection, command, length, 0);

    // check receive.
    int ret_recv, recvbytes = 0, cnt = 0;
    char buf[DEF_BUFSIZE_1K];
    // check receive#1 for header.
    if (_rmc_check_recv(connection) != 1)
    {
        fprintf(stderr, "Error in _rmc_get_command() %d - %s\n", connection, strerror(connection));
        valinfo.cause = RMC_RECV_ERROR;
        valinfo.error = TRUE;
        return valinfo;
    }

    // get header.
    while (cnt < line)
    {
        ret_recv = recv(connection, buf + recvbytes, 1, 0);
        if (ret_recv < 1)
            break;
	*(buf + recvbytes + 1) = '\0'; // because rmc_index_of_string() use the strstr()

        if (rmc_index_of_string(buf, RMC_STATUS_SERVER_ERROR     ) > -1 ||
            rmc_index_of_string(buf, RMC_STATUS_NOT_STORED_CRLF  ) > -1 ||
            rmc_index_of_string(buf, RMC_EMPTY_VALUE             ) > -1 )
            break;

        if (buf[recvbytes] == RMC_CHR_CODE_LF)
            cnt++;

        recvbytes += ret_recv;
    }

    // check server-error.    
    if (rmc_index_of_string(buf, RMC_STATUS_SERVER_ERROR) > -1) {
        _rmc_gets(connection, buf);
        valinfo.error = TRUE;
        valinfo.cause = RMC_RECV_ERROR;
        return valinfo;
    }
    if (rmc_index_of_string(buf, RMC_STATUS_NOT_STORED_CRLF) > -1 ||
        rmc_index_of_string(buf, RMC_EMPTY_VALUE           ) > -1 )
    {
        return valinfo;
    }

    // get bytes.
    int bufsize = 0;
    if (line == 1)
    {
        bufsize = _rmc_get_body_length(buf);
    }
    else if (line == 3)
    {
        char *str_line = rmc_strpbrk(rmc_strpbrk(buf, RMC_CRLF, 2), RMC_CRLF, 2);
        bufsize = _rmc_get_body_length(str_line);
    }

    // check receive#2 for body.
    if (_rmc_check_recv(connection) != 1)
    {
        fprintf(stderr, "Error in _rmc_get_command() %d - %s\n", connection, strerror(connection));
        valinfo.cause = RMC_RECV_ERROR;
        valinfo.error = TRUE;
        return valinfo;
    }

    // get body.
    recvbytes = 0;
    int body_len = bufsize;
    bufsize += strlen(RMC_RECV_END_VALUE);

    char *body = (char *)calloc(bufsize, sizeof(char));
    while (recvbytes < bufsize)
    {
        ret_recv = recv(connection, body + recvbytes, bufsize - recvbytes, 0);
        if (ret_recv < 1)
            break;

        recvbytes += ret_recv;
    }

    int i;
    char *result = (char *)calloc(body_len, sizeof(char));
    for (i = 0; i < body_len; i++)
        result[i] = body[i];
    free(body);

    valinfo.length = body_len;
    valinfo.value = result;
    return (valinfo);
}

/**
 * merge charcters with byte array.
 * @param base_command
 * @param value
 * @return command
 */
static char *_merge_chars_with_byte_array(const char *base_command, rmc_value_info valinfo)
{
    int base_bytes  = strlen(base_command);
    int value_bytes = valinfo.length;
    int total_len = base_bytes + value_bytes + 2;
    char command[total_len];

    int i, p = 0;
    for (i = 0; i < base_bytes; i++)
    {
        command[p++] = base_command[i];
    }
    for (i = 0; i < value_bytes; i++)
    {
        command[p++] = valinfo.value[i];
    }
    command[p+1] = RMC_CHR_CODE_CR;
    command[p+2] = RMC_CHR_CODE_LF;

    char *result = (char *)malloc(base_bytes + value_bytes + RMC_CRLF_SIZE);
    memcpy(result, &command[0], total_len);

    return (result);
}

/**
 * send a mklhash-command.
 *
 * @param[in]  roma_connection
 * @param[out] mklhash
 */
char *rmc_send_route_mklhash(const int connection)
{
    char *mklhash =
        _rmc_send_command(
            connection, RMC_CMD_MKLHASH, strlen(RMC_CMD_MKLHASH), DEF_BUFSIZE_4K);
    mklhash[strlen(mklhash)-1] = '\0';
    return (mklhash);
}

/**
 * get status code.
 * @param ret_value
 * @return status
 */
static int _get_status_code(const char *ret_value)
{
    int status = 0;

    if      (rmc_index_of_string(ret_value, RMC_STATUS_STORED      ) > -1) status = RMC_STORED;
    else if (rmc_index_of_string(ret_value, RMC_STATUS_NOT_STORED  ) > -1) status = RMC_NOT_STORED;
    else if (rmc_index_of_string(ret_value, RMC_STATUS_DELETED     ) > -1) status = RMC_DELETED;
    else if (rmc_index_of_string(ret_value, RMC_STATUS_NOT_FOUND   ) > -1) status = RMC_NOT_FOUND;
    else if (rmc_index_of_string(ret_value, RMC_STATUS_CLEARED     ) > -1) status = RMC_CLEARED;
    else if (rmc_index_of_string(ret_value, RMC_STATUS_NOT_CLEARED ) > -1) status = RMC_NOT_CLEARED;
    else if (rmc_index_of_string(ret_value, RMC_STATUS_NULL        ) > -1) status = RMC_ALIST_NULL;
    else if (rmc_index_of_string(ret_value, RMC_STATUS_TRUE        ) > -1) status = RMC_ALIST_TRUE;
    else if (rmc_index_of_string(ret_value, RMC_STATUS_FALSE       ) > -1) status = RMC_ALIST_FALSE;
    return (status);
}

/**
 * post command.
 *
 * @param[in] connection
 * @param[in] command
 * @return status
 */
static int _rmc_post_command(const int connection, const char *base_command, const rmc_value_info valinfo)
{
    char *command = _merge_chars_with_byte_array(base_command, valinfo);
    int command_bytes = strlen(base_command) + valinfo.length + RMC_CRLF_SIZE;

    char *result = _rmc_send_command(connection, command, command_bytes, DEF_BUFSIZE_S);
    free(command);

    int ret_value = _get_status_code(result);
    free(result);
    return (ret_value);
}

/**
 * remove command.
 * @param connection
 * @param command
 * @return status
 */
static int _rmc_remove_command(const int connection, const char *command)
{
    char *result =
        _rmc_send_command(connection, command, strlen(command), DEF_BUFSIZE_4K);
    int ret_value = _get_status_code(result);

    free(result);
    return (ret_value);
}

/**
 * send a set command.
 *
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @param[in] value
 * @param[in] exptime
 * @return [OK|NG]
 */
int rmc_send_set(
    const int connection, const char *key, const unsigned int digest,
    const rmc_value_info valinfo, const int exptime)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_SET, key, digest, exptime, valinfo.length);

    int ret = _rmc_post_command(connection, base_command, valinfo);
    return ret;
}

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
    const rmc_value_info valinfo, const int exptime)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_ADD, key, digest, exptime, valinfo.length);

    int ret = _rmc_post_command(connection, base_command, valinfo);
    return ret;
}

/**
 * send a replace command.
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @param[in] value
 * @param[in] exptime
 * @return [OK|NG]
 */
int rmc_send_replace(
    const int connection, const char *key, const unsigned int digest,
    const rmc_value_info valinfo, const int exptime)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_REPLACE, key, digest, exptime, valinfo.length);

    int ret = _rmc_post_command(connection, base_command, valinfo);
    return ret;
}

/**
 * send a append command.
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @param[in] value
 * @param[in] exptime
 * @return [OK|NG]
 */
int rmc_send_append(
    const int connection, const char *key, const unsigned int digest,
    const rmc_value_info valinfo, const int exptime)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_APPEND, key, digest, exptime, valinfo.length);

    int ret = _rmc_post_command(connection, base_command, valinfo);
    return ret;
}

/**
 * send a prepend command.
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @param[in] value
 * @param[in] exptime
 * @return [OK|NG]
 */
int rmc_send_prepend(
    const int connection, const char *key, const unsigned int digest,
    const rmc_value_info valinfo, const int exptime)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_PREPEND, key, digest, exptime, valinfo.length);

    int ret = _rmc_post_command(connection, base_command, valinfo);
    return ret;
}

/**
 * send a delete command.
 *
 */
int rmc_send_delete(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_DELETE, key);

    int ret = _rmc_remove_command(connection, command);
    return (ret);
}

/**
 * send a get command.
 *
 * @param[in] connection
 * @param[in] key
 * @param[in] digest
 * @return value
 *
 */
rmc_value_info rmc_send_get(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_GET, key);

    rmc_value_info valinfo =
        _rmc_get_command(connection, command, strlen(command), 1);

    int length = valinfo.length;
    char *ret_value = (char *)malloc(length + 1);
    memcpy(ret_value, valinfo.value, length);
    ret_value[length] = '\0';
    free(valinfo.value);

    rmc_value_info result_val;
    result_val.length = length;
    result_val.value = ret_value;
    return (result_val);
}

/**
 * send a inclement command.
 *
 */
int rmc_send_inclement()
{
    return (EXIT_SUCCESS); //@TODO
}

/**
 * send a decrement command.
 *
 */
int rmc_send_decrement()
{
    return (EXIT_SUCCESS); //@TODO
}

/**
 * send a version command.
 *
 * @param[in]  connection
 * @return version.
 */
char *rmc_send_version(const int connection)
{
    char *version =
        _rmc_send_command(connection, RMC_CMD_VERSION, strlen(RMC_CMD_VERSION), DEF_BUFSIZE_4K);
    version[strlen(version)-1] = '\0';
    return (version);
}

/**
 * send quit command.
 * @param[in] connection
 * @return [OK|NG]
 *
 */
int rmc_send_quit(const int connection)
{
    _rmc_send_command(connection, RMC_CMD_QUIT, strlen(RMC_CMD_QUIT), DEF_BUFSIZE_4K);
    return (EXIT_SUCCESS);
}

/**
 * send routedump as yamlbytes command.
 *
 * @param[in] connection.
 * @return yamlbytes
 *
 */
static int _rmc_send_routedump_as_yamlbytes(const int connection)
{
    char *result =
        _rmc_send_command(
            connection, RMC_CMD_RTDUMP_YAML_BYTES, strlen(RMC_CMD_RTDUMP_YAML_BYTES), 128);

    int i, len = 0;
    for (i = 0; i < strlen(result); i++)
    {
        if (result[i] == RMC_CHR_CODE_CR)
            break;
        len++;
    }

    char *yamlbytes = (char *)calloc(len+1, sizeof(char));
    for (i = 0; i < len; i++)
        yamlbytes[i] = result[i];

    yamlbytes[len] = '\0';
    int bytes = atoi(yamlbytes);
    free(result);
    free(yamlbytes);
    return (bytes);
}

/**
 * receive yaml.
 * @param connection
 * @param buffer
 * @param length
 * @return receive length
 */
static int _rmc_recv_yaml(int connection, char *rbuf, int len)
{
    // check send.
    if (_rmc_check_send(connection) != 1)
    {
        fprintf(stderr, "Error in rmc_check_send() %d - %s\n", connection, strerror(connection));
        return (0);
    }
    send(connection, RMC_CMD_RTDUMP_YAML, strlen(RMC_CMD_RTDUMP_YAML), 0);

    // chech receive.
    if (_rmc_check_recv(connection) != 1)
    {
        fprintf(stderr, "Error in _rmc_recv_yaml() %d - %s\n", connection, strerror(connection));
        return (0);
    }

    // receive data.
    int recvbytes = 0, ret_recv;
    while(recvbytes < len)
    {
        ret_recv = recv(connection, rbuf + recvbytes, 1, 0);
        if(ret_recv < 1)
            return ret_recv;

        recvbytes += ret_recv;
    }
    return (recvbytes);
}

/**
 * send a routedump as yaml command.
 * @param[in] connection
 * @return routing-data.
 */
rmc_routing_data rmc_send_routedump_as_yaml(const int connection, const int nodes)
{
    int bytes = _rmc_send_routedump_as_yamlbytes(connection);

    // get yaml => make routing data.
    char *buf = (char *)calloc(bytes+1, sizeof(char));    
    int ret_bytes = _rmc_recv_yaml(connection, buf, bytes);
    if (ret_bytes != bytes)
    {
        rmc_routing_data rd;
        rd.dgst_bits = 0;
        rd.div_bits = 0;
        rd.hbits = 0;
        rd.rn = 0;
        rd.search_mask = 0;
        return (rd);
    }
    buf[bytes] = '\0';

    // trim header.
    int i, startp;
    for (i = 0; i < bytes; i++)
    {
        if (i > 0 && buf[i-1] == RMC_CHR_CODE_LF)
        {
            startp = i;
            break;
        }
    }
    int bodylen = bytes - startp - strlen(RMC_EMPTY_VALUE);

    // extract body.
    char *result = (char *)malloc(bodylen + 1);
    memcpy(result, &buf[startp], bodylen);
    result[bodylen] = '\0';

    rmc_routing_data rd2 = rmc_generate_routing_data(result, nodes);
    free(buf);
    free(result);
    return (rd2);
}

/**
 * get alist's value.
 * @param connection
 * @param command
 * @param len_line
 * @return value info
 */
static rmc_value_info _rmc_get_alist_value(
    const int connection, const char *command, const int len_lines)
{
    rmc_value_info valinfo =
        _rmc_get_command(connection, command, strlen(command), len_lines);

    int length = valinfo.length;
    char *ret_value = (char *)malloc(length + 1);
    memcpy(ret_value, valinfo.value, length);
    ret_value[length] = '\0';
    free(valinfo.value);

    rmc_value_info result_val;
    result_val.length = length;
    result_val.value = ret_value;
    return (result_val);
}

/**
 * alist operation
 * @param connection
 * @param valinfo
 * @return status
 *
 */
static int _rmc_alist_operate(
    const int connection, const char *base_command, rmc_value_info valinfo)
{
    char *ret_command = _merge_chars_with_byte_array(base_command, valinfo);
    int command_bytes = strlen(base_command) + valinfo.length + RMC_CRLF_SIZE;

    char *result = _rmc_send_command(connection, ret_command, command_bytes, DEF_BUFSIZE_S);
    free(ret_command);

    int ret_value = _get_status_code(result);
    free(result);
    return (ret_value);
}

/**
 * send alist at command.
 * @param connection
 * @param key
 * @param index
 * @return value
 */
rmc_value_info rmc_send_alist_at(const int connection, const char *key, const int index)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_AT, key, index);

    rmc_value_info result_val = _rmc_get_alist_value(connection, command, 1);
    return result_val;
}

/**
 * send alist clear command.
 * @param connection
 * @param key
 * @return status
 */
int rmc_send_alist_clear(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_CLEAR, key);

    int ret = _rmc_remove_command(connection, command);
    return (ret);
}

/**
 * send alist delete.
 * @param connection
 * @param key
 * @param valinfo
 * @return status
 */
int rmc_send_alist_delete(const int connection, const char *key, const rmc_value_info valinfo)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_ALIST_DELETE, key, valinfo.length);
    
    int ret_value = _rmc_alist_operate(connection, base_command, valinfo);
    return (ret_value);
}

/**
 * send alist delete at command.
 * @param connection
 * @param key
 * @param index
 * @return status
 */
int rmc_send_alist_delete_at(const int connection, const char *key, const int index)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_DELETE_AT, key, index);

    int ret = _rmc_remove_command(connection, command);
    return (ret);
}

/**
 * send alist empty command.
 * @param connection
 * @param key
 * @return status
 */
int rmc_send_alist_empty(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_EMPTY, key);

    int ret = _rmc_remove_command(connection, command);
    return (ret);
}

/**
 * send alist first command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_first(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_FIRST, key);

    rmc_value_info result_val = _rmc_get_alist_value(connection, command, 1);
    return result_val;
}

/**
 * send alist include command.
 * @param connection
 * @param key
 * @param value
 * @return status
 */
int rmc_send_alist_include(
    const int connection, const char *key, const rmc_value_info valinfo)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_ALIST_INCLUDE, key, valinfo.length);
    
    int ret_value = _rmc_alist_operate(connection, base_command, valinfo);
    return (ret_value);
}

/**
 * send alist index command.
 * @param connection
 * @param key
 * @param value
 * @return index
 */
int rmc_send_alist_index(const int connection, const char *key, const rmc_value_info valinfo)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_ALIST_INDEX, key, valinfo.length);
    char *command = _merge_chars_with_byte_array(base_command, valinfo);
    int command_bytes = strlen(base_command) + valinfo.length + RMC_CRLF_SIZE;

    char *result = _rmc_send_command(connection, command, command_bytes, DEF_BUFSIZE_S);
    free(command);

    int ret = _get_status_code(result);
    if (ret == 0)
    {
        int length = strlen(result)-2;
        char ret_value[length+1];
        strncpy(ret_value, result, length);
        ret_value[length] = '\0';
        ret = atoi(ret_value);
    }
    free(result);
    return (ret);
}

/**
 * plugin - alist insert.
 * @param key
 * @param index
 * @param bytes
 * @return [OK|NG]
 */
int rmc_send_alist_insert(
    const int connection, const char *key, const int index, const rmc_value_info valinfo)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_ALIST_INSERT, key, index, valinfo.length);

    int ret_value = _rmc_alist_operate(connection, base_command, valinfo);
    return (ret_value);
}

/**
 * send alust sized insert command.
 * @param connection
 * @param key
 * @param size
 * @param value
 * @return status
 */
int rmc_send_alist_sized_insert(
    const int connection, const char *key,
    const int size, const rmc_value_info valinfo)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_ALIST_SIZED_INSERT, key, size, valinfo.length);

    int ret_value = _rmc_alist_operate(connection, base_command, valinfo);
    return (ret_value);
}

/**
 * plugin - alist join.
 * @param connection
 * @param key
 * @param separator
 * @return value
 */
rmc_value_info rmc_send_alist_join(const int connection, const char *key, const char *separator)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_JOIN, key, strlen(separator), separator);

    rmc_value_info result_val = _rmc_get_alist_value(connection, command, 3);
    return result_val;
}

/**
 * send alist to json command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_to_json(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_TO_JSON, key);

    rmc_value_info result_val = _rmc_get_alist_value(connection, command, 1);
    return result_val;
}

/**
 * send alist last command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_last(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_LAST, key);

    rmc_value_info result_val = _rmc_get_alist_value(connection, command, 1);
    return result_val;
}

/**
 * send alist length command.
 * @param connection
 * @param key
 * @return status
 */
int rmc_send_alist_length(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_LENGTH, key);

    char *result =
        _rmc_send_command(connection, command, strlen(command), DEF_BUFSIZE_S);

    int ret = _get_status_code(result);
    if (ret == 0)
    {
        int length = strlen(result)-2;
        char ret_value[length];
        strncpy(ret_value, result, length);
        ret_value[length] = '\0';
        ret = atoi(ret_value);
    }
    free(result);
    return (ret);
}

/**
 * send alist pop command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_pop(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_POP, key);

    rmc_value_info result_val = _rmc_get_alist_value(connection, command, 1);
    return result_val;
}

/**
 * send alist push command.
 * @param connection
 * @param key
 * @param value
 * @return status
 */
int rmc_send_alist_push(const int connection, const char *key, const rmc_value_info valinfo)
{
    char base_command[DEF_BUFSIZE_1K];
    sprintf(base_command, RMC_CMD_ALIST_PUSH, key, valinfo.length);

    int ret_value = _rmc_alist_operate(connection, base_command, valinfo);
    return (ret_value);
}

/**
 * send alist shift command.
 * @param connection
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_shift(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_SHIFT, key);

    rmc_value_info result_val = _rmc_get_alist_value(connection, command, 1);
    return result_val;
}

/**
 * plugin - alist to string.
 * @param key
 * @return value
 */
rmc_value_info rmc_send_alist_tostr(const int connection, const char *key)
{
    char command[DEF_BUFSIZE_1K];
    sprintf(command, RMC_CMD_ALIST_TO_STR, key);

    rmc_value_info result_val = _rmc_get_alist_value(connection, command, 3);
    return result_val;
}

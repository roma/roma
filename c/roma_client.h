/* 
 * File:   roma_client.h
 * Author: yosuke
 *
 * Created on 2009/06/26
 */

#ifndef _ROMA_CLIENT_H
#define	_ROMA_CLIENT_H

#ifdef	__cplusplus
extern "C" {
#endif

#include "roma_client_private.h"

// connect/disconnect.
int rmc_connect(const int hosts, const char** str_hosts);
int rmc_disconnect();

// core-commands.
int rmc_set(const char *key, const rmc_value_info valinfo, const int exptime);
int rmc_add(const char *key, const rmc_value_info valinfo, const int exptime);
int rmc_replace(const char *key, const rmc_value_info valinfo, const int exptime);
int rmc_append(const char *key, const rmc_value_info valinfo, const int exptime);
int rmc_prepend(const char *key, const rmc_value_info valinfo, const int exptime);

int rmc_cas(const char *key, const rmc_value_info valinfo, const int exptime);
int rmc_delete(const char *key);
rmc_value_info rmc_get(const char *key);

// list-commands.
rmc_value_info rmc_alist_at(const char *key, const int index);
int rmc_alist_clear(const char *key);
int rmc_alist_delete(const char *key, const rmc_value_info valinfo);
int rmc_alist_delete_at(const char *key, const int index);
int rmc_alist_empty(const char *key);
rmc_value_info rmc_alist_first(const char *key);
int rmc_alist_include(const char *key, const rmc_value_info valinfo);
int rmc_alist_index(const char *key, const rmc_value_info valinfo);
int rmc_alist_insert(const char *key, const int index, const rmc_value_info valinfo);
int rmc_alist_sized_insert(const char *key, const int size, const rmc_value_info valinfo);
rmc_value_info rmc_alist_join(const char *key, const char *separator);
rmc_value_info rmc_alist_to_json(const char * key);
rmc_value_info rmc_alist_last(const char *key);
int rmc_alist_length(const char *key);
rmc_value_info rmc_alist_pop(const char *key);
int rmc_alist_push(const char *key, const rmc_value_info valinfo);
rmc_value_info rmc_alist_shift(const char *key);
rmc_value_info rmc_alist_tostr(const char *key);

#ifdef	__cplusplus
}
#endif

#endif	/* _ROMA_CLIENT_H */


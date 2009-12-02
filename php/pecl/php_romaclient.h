/*
   +----------------------------------------------------------------------+
   | This source file is subject to version 3.0 of the PHP license,       |
   | that is bundled with this package in the file LICENSE, and is        |
   | available through the world-wide-web at the following url:           |
   | http://www.php.net/license/3_0.txt.                                  |
   | If you did not receive a copy of the PHP license and are unable to   |
   | obtain it through the world-wide-web, please send a note to          |
   | license@php.net so we can mail you a copy immediately.               |
   +----------------------------------------------------------------------+
   | Authors: yosuke hara <yosuke.hara@mail.rakuten.co.jp>                |
   +----------------------------------------------------------------------+
*/

/* $ Id: $ */ 

#ifndef PHP_ROMACLIENT_H
#define PHP_ROMACLIENT_H

#ifdef  __cplusplus
extern "C" {
#endif

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <php.h>

#ifdef HAVE_ROMACLIENT

#include <php_ini.h>
#include <SAPI.h>
#include <ext/standard/info.h>
#include <Zend/zend_extensions.h>
#ifdef  __cplusplus
} // extern "C" 
#endif
#include <roma/roma_client.h>
#ifdef  __cplusplus
extern "C" {
#endif

extern zend_module_entry romaclient_module_entry;
#define phpext_romaclient_ptr &romaclient_module_entry

#ifdef PHP_WIN32
#define PHP_ROMACLIENT_API __declspec(dllexport)
#else
#define PHP_ROMACLIENT_API
#endif

PHP_MINIT_FUNCTION(romaclient);
PHP_MSHUTDOWN_FUNCTION(romaclient);
PHP_RINIT_FUNCTION(romaclient);
PHP_RSHUTDOWN_FUNCTION(romaclient);
PHP_MINFO_FUNCTION(romaclient);

#ifdef ZTS
#include "TSRM.h"
#endif

#define FREE_RESOURCE(resource) zend_list_delete(Z_LVAL_P(resource))

#define PROP_GET_LONG(name)    Z_LVAL_P(zend_read_property(_this_ce, _this_zval, #name, strlen(#name), 1 TSRMLS_CC))
#define PROP_SET_LONG(name, l) zend_update_property_long(_this_ce, _this_zval, #name, strlen(#name), l TSRMLS_CC)

#define PROP_GET_DOUBLE(name)    Z_DVAL_P(zend_read_property(_this_ce, _this_zval, #name, strlen(#name), 1 TSRMLS_CC))
#define PROP_SET_DOUBLE(name, d) zend_update_property_double(_this_ce, _this_zval, #name, strlen(#name), d TSRMLS_CC)

#define PROP_GET_STRING(name)    Z_STRVAL_P(zend_read_property(_this_ce, _this_zval, #name, strlen(#name), 1 TSRMLS_CC))
#define PROP_GET_STRLEN(name)    Z_STRLEN_P(zend_read_property(_this_ce, _this_zval, #name, strlen(#name), 1 TSRMLS_CC))
#define PROP_SET_STRING(name, s) zend_update_property_string(_this_ce, _this_zval, #name, strlen(#name), s TSRMLS_CC)
#define PROP_SET_STRINGL(name, s, l) zend_update_property_stringl(_this_ce, _this_zval, #name, strlen(#name), s, l TSRMLS_CC)


PHP_FUNCTION(rmc_version);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_version_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_version_arg_info NULL
#endif

PHP_FUNCTION(rmc_connect);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_connect_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, str_hosts)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_connect_arg_info NULL
#endif

PHP_FUNCTION(rmc_disconnect);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_disconnect_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 0)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_disconnect_arg_info NULL
#endif

PHP_FUNCTION(rmc_set);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_set_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 3)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
  ZEND_ARG_INFO(0, exptime)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_set_arg_info NULL
#endif

PHP_FUNCTION(rmc_get);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_get_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_get_arg_info NULL
#endif

PHP_FUNCTION(rmc_add);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_add_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 3)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
  ZEND_ARG_INFO(0, exptime)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_add_arg_info NULL
#endif

PHP_FUNCTION(rmc_replace);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_replace_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 3)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
  ZEND_ARG_INFO(0, exptime)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_replace_arg_info NULL
#endif

PHP_FUNCTION(rmc_append);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_append_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 3)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
  ZEND_ARG_INFO(0, exptime)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_append_arg_info NULL
#endif

PHP_FUNCTION(rmc_prepend);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_prepend_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 3)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
  ZEND_ARG_INFO(0, exptime)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_prepend_arg_info NULL
#endif

PHP_FUNCTION(rmc_cas);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_cas_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 3)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
  ZEND_ARG_INFO(0, exptime)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_cas_arg_info NULL
#endif

PHP_FUNCTION(rmc_delete);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_delete_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_delete_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_at);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_at_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 2)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, index)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_at_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_clear);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_clear_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_clear_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_delete);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_delete_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 2)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_delete_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_delete_at);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_delete_at_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 2)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, index)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_delete_at_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_empty);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_empty_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_empty_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_first);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_first_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_first_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_include);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_include_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 2)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_include_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_index);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_index_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 2)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_index_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_insert);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_insert_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 3)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, index)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_insert_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_sized_insert);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_sized_insert_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 3)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, size)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_sized_insert_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_join);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_join_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 2)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, separator)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_join_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_to_json);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_to_json_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_to_json_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_last);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_last_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_last_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_length);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_length_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_length_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_pop);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_pop_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_pop_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_push);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_push_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 2)
  ZEND_ARG_INFO(0, key)
  ZEND_ARG_INFO(0, value)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_push_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_shift);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_shift_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_shift_arg_info NULL
#endif

PHP_FUNCTION(rmc_alist_to_str);
#if (PHP_MAJOR_VERSION >= 5)
ZEND_BEGIN_ARG_INFO_EX(rmc_alist_to_str_arg_info, ZEND_SEND_BY_VAL, ZEND_RETURN_VALUE, 1)
  ZEND_ARG_INFO(0, key)
ZEND_END_ARG_INFO()
#else /* PHP 4.x */
#define rmc_alist_to_str_arg_info NULL
#endif

#ifdef  __cplusplus
} // extern "C" 
#endif

#endif /* PHP_HAVE_ROMACLIENT */

#endif /* PHP_ROMACLIENT_H */


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */

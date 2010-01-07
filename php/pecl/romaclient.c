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

#include "php_romaclient.h"

#if HAVE_ROMACLIENT

/* {{{ romaclient_functions[] */
function_entry romaclient_functions[] = {
	PHP_FE(rmc_version         , rmc_version_arg_info)
	PHP_FE(rmc_connect         , rmc_connect_arg_info)
	PHP_FE(rmc_disconnect      , rmc_disconnect_arg_info)
	PHP_FE(rmc_set             , rmc_set_arg_info)
	PHP_FE(rmc_get             , rmc_get_arg_info)
	PHP_FE(rmc_add             , rmc_add_arg_info)
	PHP_FE(rmc_replace         , rmc_replace_arg_info)
	PHP_FE(rmc_append          , rmc_append_arg_info)
	PHP_FE(rmc_prepend         , rmc_prepend_arg_info)
	PHP_FE(rmc_cas             , rmc_cas_arg_info)
	PHP_FE(rmc_delete          , rmc_delete_arg_info)
	PHP_FE(rmc_alist_at        , rmc_alist_at_arg_info)
	PHP_FE(rmc_alist_clear     , rmc_alist_clear_arg_info)
	PHP_FE(rmc_alist_delete    , rmc_alist_delete_arg_info)
	PHP_FE(rmc_alist_delete_at , rmc_alist_delete_at_arg_info)
	PHP_FE(rmc_alist_empty     , rmc_alist_empty_arg_info)
	PHP_FE(rmc_alist_first     , rmc_alist_first_arg_info)
	PHP_FE(rmc_alist_include   , rmc_alist_include_arg_info)
	PHP_FE(rmc_alist_index     , rmc_alist_index_arg_info)
	PHP_FE(rmc_alist_insert    , rmc_alist_insert_arg_info)
	PHP_FE(rmc_alist_sized_insert, rmc_alist_sized_insert_arg_info)
	PHP_FE(rmc_alist_join      , rmc_alist_join_arg_info)
	PHP_FE(rmc_alist_to_json   , rmc_alist_to_json_arg_info)
	PHP_FE(rmc_alist_last      , rmc_alist_last_arg_info)
	PHP_FE(rmc_alist_length    , rmc_alist_length_arg_info)
	PHP_FE(rmc_alist_pop       , rmc_alist_pop_arg_info)
	PHP_FE(rmc_alist_push      , rmc_alist_push_arg_info)
	PHP_FE(rmc_alist_shift     , rmc_alist_shift_arg_info)
	PHP_FE(rmc_alist_to_str    , rmc_alist_to_str_arg_info)
	{ NULL, NULL, NULL }
};
/* }}} */


/* {{{ romaclient_module_entry
 */
zend_module_entry romaclient_module_entry = {
	STANDARD_MODULE_HEADER,
	"romaclient",
	romaclient_functions,
	PHP_MINIT(romaclient),     /* Replace with NULL if there is nothing to do at php startup   */ 
	PHP_MSHUTDOWN(romaclient), /* Replace with NULL if there is nothing to do at php shutdown  */
	PHP_RINIT(romaclient),     /* Replace with NULL if there is nothing to do at request start */
	PHP_RSHUTDOWN(romaclient), /* Replace with NULL if there is nothing to do at request end   */
	PHP_MINFO(romaclient),
	"0.8.0", 
	STANDARD_MODULE_PROPERTIES
};
/* }}} */

#ifdef COMPILE_DL_ROMACLIENT
ZEND_GET_MODULE(romaclient)
#endif


/* {{{ PHP_MINIT_FUNCTION */
PHP_MINIT_FUNCTION(romaclient)
{

	/* add your stuff here */

	return SUCCESS;
}
/* }}} */


/* {{{ PHP_MSHUTDOWN_FUNCTION */
PHP_MSHUTDOWN_FUNCTION(romaclient)
{

	/* add your stuff here */

	return SUCCESS;
}
/* }}} */


/* {{{ PHP_RINIT_FUNCTION */
PHP_RINIT_FUNCTION(romaclient)
{
	/* add your stuff here */

	return SUCCESS;
}
/* }}} */


/* {{{ PHP_RSHUTDOWN_FUNCTION */
PHP_RSHUTDOWN_FUNCTION(romaclient)
{
	/* add your stuff here */

	return SUCCESS;
}
/* }}} */


/* {{{ PHP_MINFO_FUNCTION */
PHP_MINFO_FUNCTION(romaclient)
{
	php_info_print_box_start(0);
	php_printf("<p>ROMA-Client, PHP Extension.</p>\n");
	php_printf("<p>Version 0.8.0stable (2009-07-07)</p>\n");
	php_printf("<p><b>Authors:</b></p>\n");
	php_printf("<p>yosuke hara &lt;yosuke.hara@mail.rakuten.co.jp&gt; (lead)</p>\n");
	php_info_print_box_end();
	/* add your stuff here */

}
/* }}} */


/* {{{ proto int rmc_version()
  ROMA-Client version. */
PHP_FUNCTION(rmc_version)
{
	RETURN_LONG(0);
}
/* }}} rmc_version */


/* {{{ proto int rmc_connect(string str_hosts)
  Connect ROMA-Server. */
PHP_FUNCTION(rmc_connect)
{
	const char * str_hosts = NULL;
	int str_hosts_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &str_hosts, &str_hosts_len) == FAILURE) {
		return;
	}

	static	char *delimit = ",";
	char *strhosts = (char *)calloc(str_hosts_len+1, sizeof(char));
	strncpy(strhosts, str_hosts, str_hosts_len);
	strhosts[str_hosts_len] = '\0';

	int num = 0;
	char *p, *temp[32];

	p = strtok(strhosts, delimit);
	while (p != NULL) {
		temp[num] = (char *)calloc(strlen(p)+1, sizeof(char));
		strcpy(temp[num], p);

		p = strtok(NULL, delimit);
		num++;
	}

	int i;
	char *argv[num];
	for (i = 0; i < num; i++) {
		argv[i] = (char *)calloc(strlen(temp[i])+1, sizeof(char));
		strcpy(argv[i], temp[i]);
		free(temp[i]);
	}
	int ret = rmc_connect(num, argv);
	for (i = 0; i < num; i++)
		free(argv[i]);
	free(strhosts);	
	RETURN_LONG(ret);
}
/* }}} rmc_connect */


/* {{{ proto int rmc_disconnect()
  Disconnect ROMA-Server. */
PHP_FUNCTION(rmc_disconnect)
{
	int ret = rmc_disconnect();
	RETURN_LONG(ret);
}
/* }}} rmc_disconnect */


/* {{{ proto int rmc_set(string key, string value, int exptime)
  Set Value. */
PHP_FUNCTION(rmc_set)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;
	long exptime = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssl", &key, &key_len, &value, &value_len, &exptime) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_set(key, valinfo, exptime);
	RETURN_LONG(ret);
}
/* }}} rmc_set */


/* {{{ proto string rmc_get(string key)
  Get Value. */
PHP_FUNCTION(rmc_get)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	rmc_value_info ret = rmc_get(key);
  RETVAL_STRINGL(ret.value, ret.length, 1);
	//RETURN_STRINGL(ret.value, ret.length, 1);
  int i;
  for (i = 0; i < ret.length; i++) {
      ret.value[i] = '\0';
  }
  free(ret.value);
}
/* }}} rmc_get */


/* {{{ proto int rmc_add(string key, string value, int exptime)
  Add Value. */
PHP_FUNCTION(rmc_add)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;
	long exptime = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssl", &key, &key_len, &value, &value_len, &exptime) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_add(key, valinfo, exptime);
	RETURN_LONG(ret);
}
/* }}} rmc_add */


/* {{{ proto int rmc_replace(string key, string value, int exptime)
  Replace Value. */
PHP_FUNCTION(rmc_replace)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;
	long exptime = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssl", &key, &key_len, &value, &value_len, &exptime) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_replace(key, valinfo, exptime);
	RETURN_LONG(ret);
}
/* }}} rmc_replace */


/* {{{ proto int rmc_append(string key, string value, int exptime)
  Append Value. */
PHP_FUNCTION(rmc_append)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;
	long exptime = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssl", &key, &key_len, &value, &value_len, &exptime) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_append(key, valinfo, exptime);
	RETURN_LONG(ret);
}
/* }}} rmc_append */


/* {{{ proto int rmc_prepend(string key, string value, int exptime)
  Prepend Value. */
PHP_FUNCTION(rmc_prepend)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;
	long exptime = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssl", &key, &key_len, &value, &value_len, &exptime) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_prepend(key, valinfo, exptime);
	RETURN_LONG(ret);
}
/* }}} rmc_prepend */


/* {{{ proto int rmc_cas(string key, string value, int exptime)
  Cas Value. */
PHP_FUNCTION(rmc_cas)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;
	long exptime = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ssl", &key, &key_len, &value, &value_len, &exptime) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_cas(key, valinfo, exptime);
	RETURN_LONG(ret);
}
/* }}} rmc_cas */


/* {{{ proto int rmc_delete(string key)
  Delete Value. */
PHP_FUNCTION(rmc_delete)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	int ret = rmc_delete(key);
	RETURN_LONG(ret);
}
/* }}} rmc_delete */

/*                             */
/* ====== plugin - alist ===== */
/*                             */
/* {{{ proto string rmc_alist_at(string key, int index)
  roma-client alist at. */
PHP_FUNCTION(rmc_alist_at)
{
	const char * key = NULL;
	int key_len = 0;
	long index = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sl", &key, &key_len, &index) == FAILURE) {
		return;
	}

	rmc_value_info ret = rmc_alist_at(key, index);
  RETVAL_STRINGL(ret.value, ret.length, 1);
	//RETURN_STRINGL(ret.value, ret.length, 1);
  int i;
  for (i = 0; i < ret.length; i++) {
      ret.value[i] = '\0';
  }
  free(ret.value);
}
/* }}} rmc_alist_at */


/* {{{ proto int rmc_alist_clear(string key)
  roma-client alist clear. */
PHP_FUNCTION(rmc_alist_clear)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	int ret = rmc_alist_clear(key);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_clear */


/* {{{ proto int rmc_alist_delete(string key, string value)
  roma-client alist delete. */
PHP_FUNCTION(rmc_alist_delete)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss", &key, &key_len, &value, &value_len) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_alist_delete(key, valinfo);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_delete */


/* {{{ proto int rmc_alist_delete_at(string key, int index)
  roma-client alist delete at. */
PHP_FUNCTION(rmc_alist_delete_at)
{
	const char * key = NULL;
	int key_len = 0;
	long index = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sl", &key, &key_len, &index) == FAILURE) {
		return;
	}

	int ret = rmc_alist_delete_at(key, index);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_delete_at */


/* {{{ proto int rmc_alist_empty(string key)
  roma-client alist empty. */
PHP_FUNCTION(rmc_alist_empty)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	int ret = rmc_alist_empty(key);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_empty */


/* {{{ proto string rmc_alist_first(string key)
  roma-client alist first. */
PHP_FUNCTION(rmc_alist_first)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	rmc_value_info ret = rmc_alist_first(key);
  RETVAL_STRINGL(ret.value, ret.length, 1);
	//RETURN_STRINGL(ret.value, ret.length, 1);
  int i;
  for (i = 0; i < ret.length; i++) {
      ret.value[i] = '\0';
  }
  free(ret.value);
}
/* }}} rmc_alist_first */


/* {{{ proto int rmc_alist_include(string key, string value)
  roma-client alist include. */
PHP_FUNCTION(rmc_alist_include)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss", &key, &key_len, &value, &value_len) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_alist_include(key, valinfo);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_include */


/* {{{ proto int rmc_alist_index(string key, string value)
  roma-client alist index. */
PHP_FUNCTION(rmc_alist_index)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss", &key, &key_len, &value, &value_len) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_alist_index(key, valinfo);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_index */


/* {{{ proto int rmc_alist_insert(string key, int index, string value)
  roma-client alist insert. */
PHP_FUNCTION(rmc_alist_insert)
{
	const char * key = NULL;
	int key_len = 0;
	long index = 0;
	const char * value = NULL;
	int value_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sls", &key, &key_len, &index, &value, &value_len) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_alist_insert(key, index, valinfo);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_insert */


/* {{{ proto int rmc_alist_sized_insert(string key, int size, string value)
  roma-client alist sized insert. */
PHP_FUNCTION(rmc_alist_sized_insert)
{
	const char * key = NULL;
	int key_len = 0;
	long size = 0;
	const char * value = NULL;
	int value_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "sls", &key, &key_len, &size, &value, &value_len) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_alist_sized_insert(key, size, valinfo);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_sized_insert */


/* {{{ proto string rmc_alist_join(string key, string separator)
  roma-client alist join. */
PHP_FUNCTION(rmc_alist_join)
{
	const char * key = NULL;
	int key_len = 0;
	const char * separator = NULL;
	int separator_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss", &key, &key_len, &separator, &separator_len) == FAILURE) {
		return;
	}

	rmc_value_info ret = rmc_alist_join(key, separator);
  RETVAL_STRINGL(ret.value, ret.length, 1);
	//RETURN_STRINGL(ret.value, ret.length, 1);
  int i;
  for (i = 0; i < ret.length; i++) {
      ret.value[i] = '\0';
  }
  free(ret.value);
}
/* }}} rmc_alist_join */


/* {{{ proto string rmc_alist_to_json(string key)
  roma-client alist to json. */
PHP_FUNCTION(rmc_alist_to_json)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	rmc_value_info ret = rmc_alist_to_json(key);
  RETVAL_STRINGL(ret.value, ret.length, 1);
	//RETURN_STRINGL(ret.value, ret.length, 1);
  int i;
  for (i = 0; i < ret.length; i++) {
      ret.value[i] = '\0';
  }
  free(ret.value);
}
/* }}} rmc_alist_to_json */


/* {{{ proto string rmc_alist_last(string key)
  roma-client alist last. */
PHP_FUNCTION(rmc_alist_last)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	rmc_value_info ret = rmc_alist_last(key);
  RETVAL_STRINGL(ret.value, ret.length, 1);
	//RETURN_STRINGL(ret.value, ret.length, 1);
  int i;
  for (i = 0; i < ret.length; i++) {
      ret.value[i] = '\0';
  }
  free(ret.value);
}
/* }}} rmc_alist_last */


/* {{{ proto int rmc_alist_length(string key)
  roma-client alist length. */
PHP_FUNCTION(rmc_alist_length)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	int ret = rmc_alist_length(key);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_length */


/* {{{ proto string rmc_alist_pop(string key)
  roma-client alist pop. */
PHP_FUNCTION(rmc_alist_pop)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	rmc_value_info ret = rmc_alist_pop(key);
  RETVAL_STRINGL(ret.value, ret.length, 1);
	//RETURN_STRINGL(ret.value, ret.length, 1);
  int i;
  for (i = 0; i < ret.length; i++) {
      ret.value[i] = '\0';
  }
  free(ret.value);
}
/* }}} rmc_alist_pop */


/* {{{ proto int rmc_alist_push(string key, string value)
  roma-client alist push. */
PHP_FUNCTION(rmc_alist_push)
{
	const char * key = NULL;
	int key_len = 0;
	const char * value = NULL;
	int value_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss", &key, &key_len, &value, &value_len) == FAILURE) {
		return;
	}

	rmc_value_info valinfo;
	valinfo.length = value_len;
	valinfo.value = value;

	int ret = rmc_alist_push(key, valinfo);
	RETURN_LONG(ret);
}
/* }}} rmc_alist_push */


/* {{{ proto string rmc_alist_shift(string key)
  roma-client alist shift. */
PHP_FUNCTION(rmc_alist_shift)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	rmc_value_info ret = rmc_alist_shift(key);
  RETVAL_STRINGL(ret.value, ret.length, 1);
	//RETURN_STRINGL(ret.value, ret.length, 1);
  int i;
  for (i = 0; i < ret.length; i++) {
      ret.value[i] = '\0';
  }
  free(ret.value);
}
/* }}} rmc_alist_shift */


/* {{{ proto string rmc_alist_to_str(string key)
  roma-client alist to str. */
PHP_FUNCTION(rmc_alist_to_str)
{
	const char * key = NULL;
	int key_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &key, &key_len) == FAILURE) {
		return;
	}

	rmc_value_info ret = rmc_alist_tostr(key);
  RETVAL_STRINGL(ret.value, ret.length, 1);
	//RETURN_STRINGL(ret.value, ret.length, 1);
  int i;
  for (i = 0; i < ret.length; i++) {
      ret.value[i] = '\0';
  }
  free(ret.value);
}
/* }}} rmc_alist_to_str */

#endif /* HAVE_ROMACLIENT */


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */

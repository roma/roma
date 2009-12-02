dnl
dnl $ Id: $
dnl

PHP_ARG_WITH(romaclient, whether romaclient is available,[  --with-romaclient[=DIR] With romaclient support])


if test "$PHP_ROMACLIENT" != "no"; then


  if test -r "$PHP_ROMACLIENT/include/roma/roma_client.h"; then
	PHP_ROMACLIENT_DIR="$PHP_ROMACLIENT"
  else
	AC_MSG_CHECKING(for romaclient in default path)
	for i in /usr /usr/local; do
	  if test -r "$i/include/roma/roma_client.h"; then
		PHP_ROMACLIENT_DIR=$i
		AC_MSG_RESULT(found in $i)
		break
	  fi
	done
	if test "x" = "x$PHP_ROMACLIENT_DIR"; then
	  AC_MSG_ERROR(not found)
	fi
  fi

  PHP_ADD_INCLUDE($PHP_ROMACLIENT_DIR/include)

  export OLD_CPPFLAGS="$CPPFLAGS"
  export CPPFLAGS="$CPPFLAGS $INCLUDES -DHAVE_ROMACLIENT"
  AC_CHECK_HEADER([roma/roma_client.h], [], AC_MSG_ERROR('roma/roma_client.h' header not found))
  PHP_SUBST(ROMACLIENT_SHARED_LIBADD)

  PHP_ADD_LIBRARY_WITH_PATH(romaclient, $PHP_ROMACLIENT_DIR/lib, ROMACLIENT_SHARED_LIBADD)
  export CPPFLAGS="$OLD_CPPFLAGS"

  export OLD_CPPFLAGS="$CPPFLAGS"
  export CPPFLAGS="$CPPFLAGS $INCLUDES -DHAVE_ROMACLIENT"

  AC_MSG_CHECKING(PHP version)
  AC_TRY_COMPILE([#include <php_version.h>], [
#if PHP_VERSION_ID < 40000
#error  this extension requires at least PHP version 4.0.0
#endif
],
[AC_MSG_RESULT(ok)],
[AC_MSG_ERROR([need at least PHP 4.0.0])])

  export CPPFLAGS="$OLD_CPPFLAGS"


  PHP_SUBST(ROMACLIENT_SHARED_LIBADD)
  AC_DEFINE(HAVE_ROMACLIENT, 1, [ ])

  PHP_NEW_EXTENSION(romaclient, romaclient.c , $ext_shared)

fi


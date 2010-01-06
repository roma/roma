/*
 * ROMA client
 * File:   roma_common.h
 * Author: yosuke hara
 *
 */

#ifndef _ROMA_COMMON_H
#define	_ROMA_COMMON_H

#ifdef	__cplusplus
extern "C" {
#endif

int rmc_index_of_string(const char *s1, const char *s2);

char *rmc_strpbrk(const char *s1, const char *s2, const int length);

#ifdef	__cplusplus
}
#endif

#endif	/* _ROMA_COMMON_H */

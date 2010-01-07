/*
 * ROMA client
 * File:   roma_common.c
 * Author: yosuke hara
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * index of string
 * @param s1
 * @param s2
 * @return index of string. (0 | 1 <= )
 */
int rmc_index_of_string(const char *s1, const char *s2)
{
    const char *buff1 = s1;
    const char *buff2 = s2;

    char *ans = strstr(buff1, buff2);
    int ret = -1;
    if (ans != NULL)
    {
        ret = (int)(ans - buff1);
    }
    return (ret);
}

/**
 * roma-strpbrk
 * @param s1
 * @param s2
 * @param length
 * @return
 */
char *rmc_strpbrk(const char *s1, const char *s2, const int length)
{
    for (; *s1; s1++)
    {
        const char *t = s2;
        for (; *t; t++)
        {
            if (*t == *s1){
                int i;
                for (i = 0; i < length; i++)
                {
                    s1++;
                }
                return ((char *)s1);
            }
        }
    }
    return (NULL);
}

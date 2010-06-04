/*
 * File:   roma_connection.c
 * Author: yosuke hara
 *
 * Created on 2009/06/25
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <inttypes.h>
#include <fcntl.h>
#include <errno.h>
#include "roma_client_private.h"

rmc_host_info *rmc_romahosts;
int rmc_number_of_hosts;

/**
 * get connection.
 * @param[in] host
 * @param[in] port
 * @return connection.
 */
static int _rmc_get_connection(char *host, int port)
{
    struct sockaddr_in server;
    struct hostent* entp;

    if((entp = gethostbyname( host )) == NULL ){
        return -1;
    }

    memset(&server, 0, sizeof(server));

    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr(host);
    server.sin_port = htons(port);
    bcopy(entp->h_addr, (char *)&server.sin_addr, entp->h_length);

    int s = socket(AF_INET, SOCK_STREAM, 0);
    if(s < 0){
        return -1;
    }

    int flag = fcntl(s, F_GETFL, 0);
    fcntl(s, F_SETFL, O_NONBLOCK|flag);
    int con = connect(s, (struct sockaddr *)&server, sizeof(server));

    int valopt;
    fd_set fdset;
    struct timeval tv;
    socklen_t lon;

    if (con < 0) {
        if (errno == EINPROGRESS) {
            tv.tv_sec = RMC_TIMEOUT;
            tv.tv_usec = 0;
            FD_ZERO(&fdset);
            FD_SET(s, &fdset);
            if (select(s+1, NULL, &fdset, NULL, &tv) > 0) {
                lon = sizeof(int);
                getsockopt(s, SOL_SOCKET, SO_ERROR, (void*)(&valopt), &lon);
                if (valopt) {
                    fprintf(stderr, "Error in connection() %d - %s\n", valopt, strerror(valopt));
                    return -1;
                }
            }
            else {
                fprintf(stderr, "Timeout or error() %d - %s\n", valopt, strerror(valopt));
                return -1;
            }
         }
         else {
            fprintf(stderr, "Error connecting %d - %s\n", errno, strerror(errno));
            return -1;
         }
    }
    flag = fcntl(s, F_GETFL, NULL);
    flag &= (~O_NONBLOCK);
    fcntl(s, F_SETFL, flag);
    return s;
}

/**
 * connect roma server.
 *
 * @param[in] hosts
 * @param[in] str_romahosts
 * @return [OK|NG]
 */
int connect_roma_server(const int hosts, const char **str_romahosts)
{
    if (hosts == 0) return (EXIT_FAILURE);

    rmc_number_of_hosts = hosts;
    rmc_romahosts = (rmc_host_info *)calloc(hosts, sizeof(rmc_host_info));

    int i, errors = 0;
    char *ahosts[hosts];
    for (i = 0; i < hosts; i++) {
        ahosts[i] = (char *)calloc(strlen(str_romahosts[i])+1, sizeof(char));
        strcpy(ahosts[i], str_romahosts[i]);
    }
    for (i = 0; i < hosts; i++) {
        char *str_host = ahosts[i];
        char *delimiter = "_";
        char *token = strtok(str_host, delimiter);

        int j = 0;
        while (token != NULL) {
            if (j==0) rmc_romahosts[i].ip_address = token;
            else      rmc_romahosts[i].port = atoi(token);

            token = strtok(NULL, delimiter);
            j++;
        }
        int con =
            _rmc_get_connection(rmc_romahosts[i].ip_address, rmc_romahosts[i].port);
        if (con == -1)
            errors++;

        rmc_romahosts[i].connection = con;
    }
    return (errors == hosts ? EXIT_FAILURE : EXIT_SUCCESS);
}

/**
 * select a alive node in rmc_romahosts structure by the random method.
 * @return address and portnumber string (must be free after using)
 */
char * rmc_select_node_by_rand()
{
  int i, cnt, n;
  int alive_hosts_index[rmc_number_of_hosts];
  struct timeval now;

  cnt = 0;
  for (i = 0; i < rmc_number_of_hosts; i++){
    if(rmc_romahosts[i].connection > 0){
      alive_hosts_index[cnt ++] = i;
    }
  }
  if (gettimeofday(&now, NULL) == 0)
    srand(now.tv_usec);
  else
    srand(time(NULL));
  n = alive_hosts_index[rand() % cnt];

  char *ret = malloc(strlen(rmc_romahosts[n].ip_address) + 10);
  sprintf(ret,"%s_%d",rmc_romahosts[n].ip_address,rmc_romahosts[n].port);

  return ret;
}

/**
 * roma-client - get connection.
 * @param[in] host and port.
 */
int rmc_get_connection(char *host_and_port)
{
    if (rmc_number_of_hosts == 0) return -1;
    
    int i, con = 0;
    for (i = 0; i < rmc_number_of_hosts; i++)
    {
        int check = 0;

        char str[strlen(host_and_port) + 1];
        strcpy(str, host_and_port);
        static char *delimiter = "_";
        char *token = strtok(str, delimiter);

        int j = 0;
        while (token != NULL)
        {
            if      (j==0 && strcmp(token, rmc_romahosts[i].ip_address) ==0) check++;
            else if (atoi(token) == rmc_romahosts[i].port) check++;
            if (check == 2) {
                con = rmc_romahosts[i].connection;
                break;
            }
            token = strtok(NULL, delimiter);
            j++;
        }
        if (con > 0) break;
    }
    //if (con == 0 && rmc_number_of_hosts > 0) {
    //    con = rmc_romahosts[0].connection;
    //}
    return (con);
}

/**
 * disconnect roma server.
 *
 * @return
 */
int disconnect_roma_server()
{
    int i;
    for (i = 0; i < rmc_number_of_hosts; i++)
    {
        free(rmc_romahosts[i].ip_address);
        if(rmc_romahosts[i].connection > 0)
	  close(rmc_romahosts[i].connection);
    }
    
    free(rmc_romahosts);
    return (EXIT_SUCCESS);
}

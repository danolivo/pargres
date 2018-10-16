/*
 * connection.h
 *
 *  Created on: 8 окт. 2018 г.
 *      Author: andrey
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_

#include "access/htup.h"

extern int node_number;
extern int nodes_at_cluster;

#define NODES_MAX_NUM	(1024)

extern void CONN_Init_module(void);
extern int CONN_Init_socket(int port);
extern int CONN_Set_all(void);
extern int CONN_Init_execution(void);
extern int CONN_Launch_query(const char *query);
extern void CONN_Check_query_result(void);
extern void CONN_Init_exchange(pgsocket *read_sock, pgsocket *write_sock);
extern void CONN_Exchange_close(pgsocket *write_sock);
extern void CONN_Parse_ports(char *ports);
extern void CONN_Send(pgsocket sock, void *buf, int size);
extern HeapTuple CONN_Recv(pgsocket *socks, int *res);

#endif /* CONNECTION_H_ */

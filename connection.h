/*
 * connection.h
 *
 *  Created on: 8 окт. 2018 г.
 *      Author: andrey
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_

extern int node_number;
extern int nodes_at_cluster;
extern const int EXCHANGE_BASE_PORT;

extern void CONN_Init_module(void);
extern int CONN_Init_socket(void);
extern int CONN_Set_all(void);
extern int CONN_Init_execution(int tag);
extern int CONN_Launch_query(const char *query);

#endif /* CONNECTION_H_ */

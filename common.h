/*
 * common.h
 *
 *  Created on: 9 окт. 2018 г.
 *      Author: andrey
 */

#ifndef COMMON_H_
#define COMMON_H_

#include "nodes/pg_list.h"
#include "storage/lock.h"


#define MAX_EXCHANGE_PORTS	(2000)

typedef struct
{
	LWLock	lock;
	int 	size;
	int 	index;
	int		values[MAX_EXCHANGE_PORTS];
} PortStack;


/* GUC variables */
extern int		node_number;
extern int		nodes_at_cluster;
extern char		*pargres_hosts_string;
extern int		ports_pool_size;

extern PortStack *PORTS;
extern int CoordinatorNode;
extern bool PargresInitialized;


extern void STACK_Init(PortStack *stack, int range_min, int size);
int STACK_Pop(PortStack *stack);
void STACK_Push(PortStack *stack, int value);

#endif /* COMMON_H_ */

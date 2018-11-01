/*
 * common.c
 *
 *      Author: andrey
 */

#include "postgres.h"

#include "common.h"


/* GUC variables */
int node_number;
int nodes_at_cluster;
char	*pargres_hosts_string = NULL;
int		ports_pool_size = 100;

int CoordinatorNode = -1;
bool PargresInitialized = false;
PortStack *PORTS;


void
STACK_Init(PortStack *stack, int range_min, int size)
{
	int i;

	LWLockAcquire(&stack->lock, LW_EXCLUSIVE);

	stack->size = size;
	stack->index = 0;
	for (i = 0; i < stack->size; i++)
		stack->values[i] = range_min + i;

	LWLockRelease(&stack->lock);
}

int
STACK_Pop(PortStack *stack)
{
	int value;

	LWLockAcquire(&stack->lock, LW_EXCLUSIVE);

	Assert(stack->index < stack->size);
	value = stack->values[stack->index++];

	LWLockRelease(&stack->lock);
	return value;
}

void
STACK_Push(PortStack *stack, int value)
{
	LWLockAcquire(&stack->lock, LW_EXCLUSIVE);

	Assert(stack->index > 0);
	stack->values[--stack->index] = value;

	LWLockRelease(&stack->lock);
}

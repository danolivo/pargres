/* ------------------------------------------------------------------------
 *
 * hooks_exec.c
 *		Executor-related logic of the ParGRES extension.
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common.h"
#include "connection.h"
#include "exchange.h"
#include "hooks_exec.h"


static ExecutorStart_hook_type	prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type	prev_ExecutorEnd = NULL;


static void HOOK_ExecStart_injection(QueryDesc *queryDesc, int eflags);
static void HOOK_ExecEnd_injection(QueryDesc *queryDesc);


void
EXEC_Hooks_init(void)
{
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = HOOK_ExecStart_injection;

	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = HOOK_ExecEnd_injection;
}

static void
HOOK_ExecStart_injection(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	ProcessSharedConnInfoPool.size = -1;
}

static void
HOOK_ExecEnd_injection(QueryDesc *queryDesc)
{
	/* Execute before hook because it destruct memory context of exchange list */
	if (PargresInitialized)
	{
		OnExecutionEnd();

		if (CoordNode == node_number)
			CONN_Check_query_result();
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

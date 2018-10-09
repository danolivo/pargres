/*
 * hooks_exec.c
 *
 *      Author: andrey
 */

#include "postgres.h"

#include "common.h"
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

	elog(LOG, "HOOK_ExecStart_injection: %d", list_length(ExchangeNodesPrivate));
}

static void
HOOK_ExecEnd_injection(QueryDesc *queryDesc)
{
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

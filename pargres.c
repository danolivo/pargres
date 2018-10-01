#include "postgres.h"

#include "tcop/utility.h"

#include "pargres.h"

PG_MODULE_MAGIC;

/*
 * Declarations
 */
void _PG_init(void);

typedef int (*fragmentation_fn_t) (int value, int nnodes);

typedef struct
{
	Oid					relid;
	fragmentation_fn_t	func;
} fragmentation_functions;

int nfrfuncs = 0;
fragmentation_functions frfuncs[1000];

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static int fragmentation_fn_default(int value, int nnodes);

static int
fragmentation_fn_default(int value, int nnodes)
{
	return value%nnodes;
}

/*
 * sepgsql_utility_command
 *
 * It hooks CREATE TABLE command to add a fragmentation function for it.
 */
static void
sepgsql_utility_command(PlannedStmt *pstmt,
						const char *queryString,
						ProcessUtilityContext context,
						ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						char *completionTag)
{
	Assert(nfrfuncs < 100);

	elog(LOG, "Hello World! It's my first extension!");
	frfuncs[nfrfuncs].func = fragmentation_fn_default;

	nfrfuncs++;

	if (next_ProcessUtility_hook)
		(*next_ProcessUtility_hook) (pstmt, queryString, context, params, queryEnv,
								 	 dest, completionTag);
	else
	{
		elog(LOG, "ZERO Utility!");
		standard_ProcessUtility(pstmt, queryString,
											context, params, queryEnv,
											dest, completionTag);
	}
}

/*
 * Module load/unload callback
 */
void
_PG_init(void)
{
	/* ProcessUtility hook */
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = sepgsql_utility_command;
}

#include "postgres.h"

#include "nodes/makefuncs.h"
#include "tcop/utility.h"

#include "unistd.h"
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

/*
 * GUC: node_number
 */
static int node_number;
static int nodes_at_cluster;

static int
fragmentation_fn_default(int value, int nnodes)
{
	return value%nnodes;
}

#define NODES_MAX_NUM	(1024)

static void
set_sequence_options(CreateSeqStmt *seq, int nnum)
{
	ListCell	*option;
	int64		base;

	base = nnum*PG_INT32_MAX/NODES_MAX_NUM;

	foreach(option, seq->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "start") == 0)
			elog(ERROR, "Sequence: start can't be used");
		else if (strcmp(defel->defname, "minvalue") == 0)
			elog(ERROR, "Sequence: minvalue can't be used");
		else if (strcmp(defel->defname, "maxvalue") == 0)
			elog(ERROR, "Sequence: maxvalue can't be used");
	}

	seq->options = lappend(seq->options,
						  makeDefElem("maxvalue", (Node *) makeFloat(psprintf(INT64_FORMAT, base + PG_INT32_MAX/NODES_MAX_NUM - 1)), -1));
	seq->options = lappend(seq->options,
						  makeDefElem("minvalue", (Node *) makeFloat(psprintf(INT64_FORMAT, base)), -1));
	seq->options = lappend(seq->options,
						  makeDefElem("start", (Node *) makeFloat(psprintf(INT64_FORMAT, base)), -1));
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
	Node	*parsetree = pstmt->utilityStmt;

	Assert(nfrfuncs < 100);

	elog(LOG, "Hello World! It's my first extension!");
	frfuncs[nfrfuncs].func = fragmentation_fn_default;

	nfrfuncs++;

	switch (nodeTag(parsetree))
	{
	case T_CreateSeqStmt: /* CREATE SEQUENCE */
		elog(LOG, "CREATE SEQUENCE! %d", getpid());
		set_sequence_options((CreateSeqStmt *) parsetree, node_number);
		break;
	case T_CreateStmt: /* CREATE TABLE */
		elog(LOG, "CREATE TABLE! %d", getpid());
		break;
	default:
		break;
	}

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
	DefineCustomIntVariable("pargres.node",
							"Node number in instances collaboration",
							NULL,
							&node_number,
							1,
							0,
							1023,
							PGC_SIGHUP,
							GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pargres.nnodes",
							"Nodes number in instances collaboration",
							NULL,
							&nodes_at_cluster,
							2,
							2,
							1024,
							PGC_SIGHUP,
							GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	/* ProcessUtility hook */
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = sepgsql_utility_command;
}

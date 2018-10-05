#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

#include "unistd.h"

#include "exchange.h"
#include "pargres.h"

PG_MODULE_MAGIC;

/*
 * Declarations
 */
void _PG_init(void);

/* This subplan is unfragmented */
const fr_options_t NO_FRAGMENTATION = {.attno = -1, .funcId = -1};

typedef struct
{
	char			relname[NAMEDATALEN];
	Oid				relid;
	fr_options_t	frOpts;
} FragRels;

int			nfrRelations = 0;
FragRels	frRelations[1000];

/* Name of relation with fragmentation options */
#define RELATIONS_FRAG_CONFIG		"relsfrag"
//#define RELATIONS_PARGRES_CONFIG	"pargres_config"

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static planner_hook_type		prev_planner_hook = NULL;

static PlannedStmt *planner_insert_exchange(Query *parse, int cursorOptions,
											ParamListInfo boundParams);

static void changeModifyTablePlan(Plan *plan, PlannedStmt *stmt,
								  fr_options_t innerFrOpts,
								  fr_options_t outerFrOpts);
static void create_table_frag(const char *relname, int attno, fr_func_id fid);
static void load_description_frag(void);

/*
 * GUC: node_number
 */
static int node_number;
static int nodes_at_cluster;

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
 * ParGRES_Utility_hooking
 *
 * It hooks CREATE TABLE command and some other to manage database distribution.
 */
static void
ParGRES_Utility_hooking(PlannedStmt *pstmt,
						const char *queryString,
						ProcessUtilityContext context,
						ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						char *completionTag)
{
	Node	*parsetree = pstmt->utilityStmt;

	Assert(nfrRelations < 100);

	switch (nodeTag(parsetree))
	{
	case T_CreateSeqStmt: /* CREATE SEQUENCE */
//		set_sequence_options((CreateSeqStmt *) parsetree, node_number);
		break;
	case T_CreateStmt: /* CREATE TABLE */
		create_table_frag(((CreateStmt *)parsetree)->relation->relname,
							1, FR_FUNC_DEFAULT);
//		add_table_fragmentation((CreateStmt *) parsetree);
		break;
	default:
		break;
	}

	if (next_ProcessUtility_hook)
		(*next_ProcessUtility_hook) (pstmt, queryString, context, params, queryEnv,
								 	 dest, completionTag);
	else
		standard_ProcessUtility(pstmt, queryString,
											context, params, queryEnv,
											dest, completionTag);
}

static void
planner_hooking(void)
{
	prev_planner_hook	= planner_hook;
	planner_hook		= planner_insert_exchange;
}

/*
 * Calls standard query planner or its previous hook.
 */
static PlannedStmt *
call_default_planner(Query *parse,
					 int cursorOptions,
					 ParamListInfo boundParams)
{
	if (prev_planner_hook)
		return prev_planner_hook(parse, cursorOptions, boundParams);
	else
		return standard_planner(parse, cursorOptions, boundParams);
}

static fr_options_t
get_fragmentation(Oid relid)
{
	char			*relname;
	Relation		relation = try_relation_open(relid, NoLock);
	fr_options_t	result = NO_FRAGMENTATION;

	if (relation)
	{
		int i = 0;

		relname = RelationGetRelationName(relation);
//		elog(LOG, "START nfrRelations=%d, %s", nfrRelations, relname);
		while ((i < nfrRelations) &&
			   (strcmp(frRelations[i].relname, relname) != 0))
		{
//			elog(LOG, "nfrRelations=%d, %s", nfrRelations, frRelations[i].relname);
			i++;
		}
		if (i == nfrRelations)
			elog(LOG, "Relation %s (relid=%d) not distributed!", relname, relid);
		else
		{
//			elog(LOG, "Distribution relid=%d found!", relid);
			if (frRelations[i].relid == InvalidOid)
				frRelations[i].relid = relid;
			result = frRelations[i].frOpts;
		}
		relation_close(relation, NoLock);
	}
	else
		elog(LOG, "Relation relid=%d not exists!", relid);

	return result;
}

static bool
isNullFragmentation(fr_options_t *frOpts)
{
	if (frOpts->funcId < 0)
		return true;
	else
		return false;
}

static bool
isEqualFragmentation(fr_options_t *frOpts1, fr_options_t *frOpts2)
{
	if (frOpts1->attno != frOpts2->attno)
		return false;
	if (frOpts1->funcId != frOpts2->funcId)
		return false;
	return true;
}

/*
 * Traverse the tree, analyze fragmentation and insert EXCHANGE nodes
 * to redistribute tuples for correct execution.
 */
static fr_options_t
traverse_tree(Plan *root, PlannedStmt *stmt)
{
	fr_options_t	innerFrOpts = NO_FRAGMENTATION,
					outerFrOpts = NO_FRAGMENTATION,
					FrOpts = NO_FRAGMENTATION;
	Oid				relid;

	check_stack_depth();

	if (innerPlan(root))
		innerFrOpts = traverse_tree(innerPlan(root), stmt);

	if (outerPlan(root))
		outerFrOpts = traverse_tree(outerPlan(root), stmt);

	switch (nodeTag(root))
	{
	case T_ModifyTable:
		changeModifyTablePlan(root, stmt, innerFrOpts, outerFrOpts);
		break;
	case T_SeqScan:
		relid = getrelid(((SeqScan *)root)->scanrelid, stmt->rtable);
//		elog(LOG, "search relid=%d", relid);
		FrOpts = get_fragmentation(relid);
		return FrOpts;
	default:
		if (!isNullFragmentation(&innerFrOpts) && !isNullFragmentation(&outerFrOpts))
			Assert(isEqualFragmentation(&innerFrOpts, &outerFrOpts));

		if (!isNullFragmentation(&innerFrOpts))
			return innerFrOpts;
		if (!isNullFragmentation(&outerFrOpts))
			return outerFrOpts;
		break;
	}

	return NO_FRAGMENTATION;
}

static void
changeModifyTablePlan(Plan *plan, PlannedStmt *stmt, fr_options_t innerFrOpts,
													fr_options_t outerFrOpts)
{
	ModifyTable	*modify_table = (ModifyTable *) plan;
	List		*rangeTable = stmt->rtable;
	Oid			resultRelationOid;

	/* Skip if not ModifyTable with 'INSERT' command */
	if (!IsA(modify_table, ModifyTable) || modify_table->operation != CMD_INSERT)
		return;

	/* Simple way for prototype only*/
	Assert(list_length(modify_table->resultRelations) == 1);
	resultRelationOid = getrelid(linitial_int(stmt->resultRelations),
																	rangeTable);
	Assert(list_length(modify_table->plans) == 1);
//elog(LOG, "changeModifyTablePlan: %d %d", node_number, nodes_at_cluster);
	/* Insert EXCHANGE node as a children of INSERT node */
	linitial(modify_table->plans) = make_exchange(linitial(modify_table->plans),
			get_fragmentation(resultRelationOid), true, node_number, nodes_at_cluster);
}

PlannedStmt *
planner_insert_exchange(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *stmt;
	Plan		*root;

	stmt = call_default_planner(parse, cursorOptions, boundParams);

	load_description_frag();

	root = stmt->planTree;
	/*
	 * Traverse a tree. We pass on a statement for mapping relation IDs.
	 */
	traverse_tree(root, stmt);

	return stmt;
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
	ProcessUtility_hook = ParGRES_Utility_hooking;

	EXCHANGE_Init();
	planner_hooking();
}

/*
 * Add a description row into the fragmentation table.
 */
static void
create_table_frag(const char *relname, int attno, fr_func_id fid)
{
	Relation	rel;
	RangeVar	*relfrag_table_rv;
	HeapTuple	tuple;
	Datum		values[3];
	bool		nulls[3] = {false, false, false};
	char		reln[64];

	if (strcmp(relname, RELATIONS_FRAG_CONFIG) == 0)
		return;

	StrNCpy(reln, relname, NAMEDATALEN);
	Assert(relname != 0);

	values[0] = CStringGetTextDatum(reln);
	values[1] = Int32GetDatum(attno);
	values[2] = Int32GetDatum(fid);

	relfrag_table_rv = makeRangeVar("public", RELATIONS_FRAG_CONFIG, -1);
	rel = heap_openrv(relfrag_table_rv, RowExclusiveLock);

	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	PG_TRY();
	{
		simple_heap_insert(rel, tuple);
	}
	PG_CATCH();
	{
		CommandCounterIncrement();
		simple_heap_delete(rel, &(tuple->t_self));
	}
	PG_END_TRY();

	heap_close(rel, RowExclusiveLock);
	CommandCounterIncrement();
}

/*
 * Load distribution rules of relations from special table
 * like nodeSeqscan.c -> SeqNext() function
 */
static void
load_description_frag(void)
{
	RangeVar		*relfrag_table_rv;
	Relation		rel;
	HeapScanDesc	scandesc;
	HeapTuple		tuple;
	Datum			values[3];
	bool			nulls[3];

	relfrag_table_rv = makeRangeVar("public", RELATIONS_FRAG_CONFIG, -1);
	rel = heap_openrv_extended(relfrag_table_rv, AccessShareLock, true);

	if (rel == NULL)
		return;

	scandesc = heap_beginscan(rel, GetTransactionSnapshot(), 0, NULL);

	nfrRelations = 0;
	for ( ; (tuple = heap_getnext(scandesc, ForwardScanDirection)) != NULL; )
	{
		heap_deform_tuple(tuple, rel->rd_att, values, nulls);
		strcpy(frRelations[nfrRelations].relname, TextDatumGetCString(values[0]));
		frRelations[nfrRelations].frOpts.attno = DatumGetInt32(values[1]);
		frRelations[nfrRelations].frOpts.funcId = DatumGetInt32(values[2]);
//		elog(LOG, "[%d] relname: %s attno: %d funcId: %d", nfrRelations,
//						frRelations[nfrRelations].relname,
//						frRelations[nfrRelations].frOpts.attno,
//						frRelations[nfrRelations].frOpts.funcId);
		nfrRelations++;
	}

	heap_endscan(scandesc);
	heap_close(rel, AccessShareLock);
}

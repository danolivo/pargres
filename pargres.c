#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

#include "unistd.h"

#include "common.h"
#include "connection.h"
#include "exchange.h"
#include "hooks_exec.h"
#include "pargres.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(set_query_id);
PG_FUNCTION_INFO_V1(set_exchange_ports);
PG_FUNCTION_INFO_V1(isLocalValue);

/*
 * Declarations
 */
void _PG_init(void);

/* This subplan is unfragmented */
const fr_options_t NO_FRAGMENTATION = {.attno = -1, .funcId = FR_FUNC_NINITIALIZED};

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

static ProcessUtility_hook_type 	next_ProcessUtility_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type			prev_planner_hook = NULL;

static void HOOK_Parser_injection(ParseState *pstate, Query *query);
static PlannedStmt *HOOK_Planner_injection(Query *parse, int cursorOptions,
											ParamListInfo boundParams);

static void changeAggPlan(Plan *plan, PlannedStmt *stmt, fr_options_t outerFrOpts);
static fr_options_t changeJoinPlan(Plan *plan, PlannedStmt *stmt,
						   fr_options_t innerFrOpts,
						   fr_options_t outerFrOpts);
static void changeModifyTablePlan(Plan *plan, PlannedStmt *stmt,
								  fr_options_t innerFrOpts,
								  fr_options_t outerFrOpts);
static void create_table_frag(const char *relname, int attno, fr_func_id fid);
static void load_description_frag(void);
static fr_options_t getRelFrag(const char *relname);

#define NODES_MAX_NUM	(1024)
/*
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
*/

static void
PLAN_Hooks_init(void)
{
	prev_planner_hook	= planner_hook;
	planner_hook		= HOOK_Planner_injection;
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

static fr_options_t
getRelFrag(const char *relname)
{
	fr_options_t	result = NO_FRAGMENTATION;
	int i = 0;

	while ((i < nfrRelations) && (strcmp(frRelations[i].relname, relname) != 0))
			i++;

	if (i < nfrRelations)
		result = frRelations[i].frOpts;

	return result;
}

static bool
isNullFragmentation(fr_options_t *frOpts)
{
	if (frOpts->funcId == FR_FUNC_NINITIALIZED)
		return true;
	else
		return false;
}

static bool
isEqualFragmentation(const fr_options_t *frOpts1, const fr_options_t *frOpts2)
{
	if (frOpts1->attno != frOpts2->attno)
		return false;
	if (frOpts1->funcId != frOpts2->funcId)
		return false;
	return true;
}

static int inner_frag_attr;
static int outer_frag_attr;

static void
traverse_qual_list(Expr *node, int inner_attno, int outer_attno, int rec)
{
//	elog(INFO, "[%d] type=%d", rec, node->type);

	switch (nodeTag(node))
	{
		case T_Var:
		{
			Var *variable = (Var *) node;

			if (variable->varattno <= 0)
			{
				/* Do not process whole-row or system columns var */
				elog(ERROR, "Qual list: Not used varattno: %d", variable->varattno);
				break;
			}
			else if (variable->varno == INNER_VAR)
			{
				if (variable->varattno != inner_attno)
				{
//					elog(INFO, "variable->varattno = %d", variable->varattno);
					inner_frag_attr = (inner_frag_attr == 0) ?
										variable->varattno : -1;
				}
			}
			else if (variable->varno == OUTER_VAR)
			{
				if (variable->varattno != outer_attno)
				{
	//				elog(INFO, "variable->varattno = %d", variable->varattno);
					outer_frag_attr = (outer_frag_attr == 0) ?
										variable->varattno : -1;
				}
			}
			break;
		}

		case T_RelabelType:
			traverse_qual_list(((RelabelType *) node)->arg,
								inner_attno, outer_attno, rec+1);
			break;
		case T_FieldStore:
			traverse_qual_list(((FieldStore *) node)->arg,
								inner_attno, outer_attno, rec+1);
			break;
		case T_CoerceViaIO:
			traverse_qual_list(((CoerceViaIO *) node)->arg,
											inner_attno, outer_attno, rec+1);
			break;
		case T_ArrayCoerceExpr:
			traverse_qual_list(((ArrayCoerceExpr *) node)->arg,
											inner_attno, outer_attno, rec+1);
			break;


		case T_BoolExpr:
		{
			BoolExpr   *boolexpr = (BoolExpr *) node;
			ListCell   *lc;

			foreach(lc, boolexpr->args)
			{
				Expr *arg = (Expr *) lfirst(lc);
				traverse_qual_list(arg, inner_attno, outer_attno, rec+1);
			}
			break;
		}
		case T_MinMaxExpr:
		{
			MinMaxExpr *minmaxexpr = (MinMaxExpr *) node;
			ListCell   *lc;

			foreach(lc, minmaxexpr->args)
			{
				Expr *arg = (Expr *) lfirst(lc);
				traverse_qual_list(arg, inner_attno, outer_attno, rec+1);
			}
			break;
		}
		case T_OpExpr:
		{
			OpExpr	   *op = (OpExpr *) node;
			ListCell   *lc;

			foreach(lc, op->args)
			{
				Expr *arg = (Expr *) lfirst(lc);
				traverse_qual_list(arg, inner_attno, outer_attno, rec+1);
			}
			break;

		}


		case T_Const:
		default:
			break;
	}
}

static void
vars(List *qual)
{
	ListCell   *lc;

	foreach(lc, qual)
	{
		Expr *node = (Expr *) lfirst(lc);

		traverse_qual_list(node, 1, 1, 0);
	}
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
		FrOpts = get_fragmentation(relid);
		return FrOpts;

	case T_Agg:
		Assert(root->righttree == NULL);
		changeAggPlan(root, stmt, outerFrOpts);
		break;
	case T_HashJoin:
	case T_MergeJoin:
	case T_NestLoop:
	{
		inner_frag_attr = 0;
		outer_frag_attr = 0;

		if (nodeTag(root) == T_HashJoin)
			vars(((HashJoin *) root)->hashclauses);
		else if (nodeTag(root) == T_MergeJoin)
					vars(((MergeJoin *) root)->mergeclauses);
		else
			vars(((Join *) root)->joinqual);
//		elog(INFO, "frag_attr: %u %u", inner_frag_attr, outer_frag_attr);

		changeJoinPlan(root, stmt, innerFrOpts, outerFrOpts);
		break;
	}
	default:
		if (!isNullFragmentation(&innerFrOpts) && !isNullFragmentation(&outerFrOpts))
		{
			if (!isEqualFragmentation(&innerFrOpts, &outerFrOpts))
				elog(LOG, "!isEqualFragmentation: tag=%d (%d %d) (%d %d) %u", nodeTag(root), innerFrOpts.attno, innerFrOpts.funcId, outerFrOpts.attno, outerFrOpts.funcId, isNullFragmentation(&innerFrOpts));
			Assert(isEqualFragmentation(&innerFrOpts, &outerFrOpts));
		}
		if (!isNullFragmentation(&innerFrOpts))
			return innerFrOpts;
		if (!isNullFragmentation(&outerFrOpts))
			return outerFrOpts;
		break;
	}

	return NO_FRAGMENTATION;
}

static void
changeAggPlan(Plan *plan, PlannedStmt *stmt, fr_options_t outerFrOpts)
{
	Agg	*agg = (Agg *) plan;

	/*
	 * In the case of simple and final aggregation we have same logic:
	 * insert exchange node below T_Agg node.
	 * Exchange below need to send each tuple to each over node, 'broadcast tuple'.
	 */
	if (!DO_AGGSPLIT_SKIPFINAL(agg->aggsplit))
	{
		Assert(plan->righttree == NULL);
		plan->lefttree = make_exchange(plan->lefttree, outerFrOpts, false, true, node_number, nodes_at_cluster);
	}
}

static fr_options_t
changeJoinPlan(Plan *plan, PlannedStmt *stmt, fr_options_t innerFrOpts,
			   fr_options_t outerFrOpts)
{
	Plan	**InnerPlan;

	if (isEqualFragmentation(&innerFrOpts, &NO_FRAGMENTATION) || isEqualFragmentation(&outerFrOpts, &NO_FRAGMENTATION))
		/* Join with system relation. Made it locally */
		return NO_FRAGMENTATION;

	if (nodeTag(plan) == T_HashJoin)
	{
		Assert(nodeTag(innerPlan(plan)) == T_Hash);
		InnerPlan = &outerPlan(innerPlan(plan));
	}
	else
	{
		InnerPlan = &innerPlan(plan);
//		elog(INFO, "-------------------ERROR!------------");
	}

	if ((inner_frag_attr < 0) || (outer_frag_attr < 0))
	{
		/* Join by more than one attribute. Broadcast inner table to all nodes */
		*InnerPlan = make_exchange(*InnerPlan, outerFrOpts, false,
										true, node_number, nodes_at_cluster);
		return outerFrOpts;
	}

	if (outerFrOpts.attno != outer_frag_attr)
	{
		if (innerFrOpts.attno == inner_frag_attr)
		{
			outerFrOpts.attno = outer_frag_attr;
			outerFrOpts.funcId = innerFrOpts.funcId;
			outerPlan(plan) = make_exchange(outerPlan(plan), outerFrOpts, false,
											false, node_number, nodes_at_cluster);
			return outerFrOpts;
		}
		else
		{
			outerFrOpts.attno = outer_frag_attr;
			innerFrOpts.attno = inner_frag_attr;
			innerFrOpts.funcId = outerFrOpts.funcId;
	//		elog(INFO, "-------------------INSERT!------------ %d %d %d %d", nodeTag(*InnerPlan), nodeTag(plan), nodeTag(outerPlan(plan)), nodeTag(innerPlan(plan)));
			outerPlan(plan) = make_exchange(outerPlan(plan), outerFrOpts, false,
											false, node_number, nodes_at_cluster);
			*InnerPlan = make_exchange(*InnerPlan, innerFrOpts, false,
														false, node_number, nodes_at_cluster);

			return outerFrOpts;
		}

	}
	else
	{
		if (innerFrOpts.attno == inner_frag_attr)
		{
			if (outerFrOpts.funcId == innerFrOpts.funcId)
				return outerFrOpts;

			innerFrOpts.funcId = outerFrOpts.funcId;
			*InnerPlan = make_exchange(*InnerPlan, innerFrOpts, false,
											false, node_number, nodes_at_cluster);
			return outerFrOpts;
		}
		else
		{
			innerFrOpts.attno = inner_frag_attr;
			innerFrOpts.funcId = outerFrOpts.funcId;
			*InnerPlan = make_exchange(*InnerPlan, innerFrOpts, false,
											false, node_number, nodes_at_cluster);
			return outerFrOpts;
		}
	}

	return innerFrOpts;
}

static void
changeModifyTablePlan(Plan *plan, PlannedStmt *stmt, fr_options_t innerFrOpts,
													fr_options_t outerFrOpts)
{
	ModifyTable	*modify_table = (ModifyTable *) plan;
	List		*rangeTable = stmt->rtable;
	Oid			resultRelationOid;
	int			nodetag = nodeTag(linitial(modify_table->plans));

	/* Skip if not ModifyTable with 'INSERT' command */
	if (!IsA(modify_table, ModifyTable) || modify_table->operation != CMD_INSERT)
		return;

	/* Simple way for prototype only*/
	Assert(list_length(modify_table->resultRelations) == 1);
	resultRelationOid = getrelid(linitial_int(stmt->resultRelations),
																	rangeTable);
	Assert(list_length(modify_table->plans) == 1);

	/* Insert EXCHANGE node as a children of INSERT node */
	if ((nodetag == T_Result) || (nodetag == T_ValuesScan))
		linitial(modify_table->plans) = make_exchange(linitial(modify_table->plans),
			get_fragmentation(resultRelationOid), true, false, node_number, nodes_at_cluster);
	else {
//		elog(LOG, "Seq Scan INSERT subplan");
		linitial(modify_table->plans) = make_exchange(linitial(modify_table->plans),
					get_fragmentation(resultRelationOid), false, false, node_number, nodes_at_cluster);
	}
}

static void
Do_Refragment_relation(const char *relname)
{
	PGconn		*conn;
	PGresult	*result;
	char		conninfo[STRING_SIZE_MAX];
	char		command[STRING_SIZE_MAX];

	sprintf(conninfo, "host=%s port=%d%c", "localhost", 5433, '\0');
	conn = PQconnectdb(conninfo);
	Assert(PQstatus(conn) != CONNECTION_BAD);

	sprintf(command,
	"INSERT INTO pgbench_accounts (SELECT * FROM %s) ON CONFLICT DO NOTHING;%c",
																relname, '\0');
	result = PQexec(conn, command);
	Assert(PQresultStatus(result) != PGRES_FATAL_ERROR);

	sprintf(command,
	"DELETE FROM %s WHERE isLocalValue('%s', aid) = false;%c",
														relname, relname, '\0');
	result = PQexec(conn, command);
	Assert(PQresultStatus(result) != PGRES_FATAL_ERROR);
}

/*
 * HOOK_Utility_injection
 *
 * It hooks CREATE TABLE command and some other to manage database distribution.
 */
static void
HOOK_Utility_injection(PlannedStmt *pstmt,
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
		create_table_frag(((CreateStmt *)parsetree)->relation->relname, 1,
						  FR_FUNC_DEFAULT);
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
	CONN_Check_query_result();

	if ((nodeTag(parsetree) == T_CopyStmt) && (CoordinatorNode == node_number))
	{
		CopyStmt *cpy = (CopyStmt *) parsetree;
//		elog(LOG, "Post COPY opts: from=%u relname=%s", cpy->is_from, cpy->relation->relname);
		if ((cpy->is_from) && (cpy->relation))
		{
//			Do_Refragment_relation(cpy->relation->relname);
		}
	}
}

/*
 * Post-parse-analysis hook.
 */
static void
HOOK_Parser_injection(ParseState *pstate, Query *query)
{
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query);

//	elog(LOG, "Parser Injection start");
	if ((query->commandType == CMD_UTILITY) && (nodeTag(query->utilityStmt) == T_CopyStmt))
		return;

	/* Extension is not initialized. */
	if (!OidIsValid(get_extension_oid("pargres", true)))
		return;

	PargresInitialized = true;

	if (strstr(pstate->p_sourcetext, "set_query_id(") != NULL)
	{
		PargresInitialized = false;
		return;
	}

	if (strstr(pstate->p_sourcetext, "set_exchange_ports(") != NULL)
	{
		PargresInitialized = false;
		return;
	}

	if (CoordinatorNode < 0)
		CoordinatorNode = node_number;

	/*
	 * If we will use backends pool this will be not working correctly.
	 */
	if (CoordinatorNode != node_number)
		return;

	/*
	 * We setup listener socket before connection. After Query sending backends
	 * from other nodes will set connection to me.
	 */
	CONN_Init_socket(0);

	/* Init connection */
	if (CONN_Set_all() < 0)
		return;

	/* Send CoordinatorId and query tag */
	CONN_Init_execution();

	CONN_Launch_query(pstate->p_sourcetext);
//	elog(LOG, "Parser Injection Finish");
}

PlannedStmt *
HOOK_Planner_injection(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt 	*stmt;
	Plan			*root;
	fr_options_t	frOpts = {.attno = 1, .funcId = FR_FUNC_GATHER};

	if (prev_planner_hook)
		stmt = prev_planner_hook(parse, cursorOptions, boundParams);
	else
		stmt = standard_planner(parse, cursorOptions, boundParams);

	if (!PargresInitialized)
		return stmt;

	load_description_frag();

	root = stmt->planTree;
	/*
	 * Traverse a tree. We pass on a statement for mapping relation IDs.
	 */
	traverse_tree(root, stmt);

	if (nodeTag(stmt->planTree) != T_Agg)
	{
		/*
		 * Aggregate node generate same value at each parallel plan by
		 * a exchange broadcasting in lefttree node. Now, We do not need to shuffle
		 * the data.
		 */
		Assert(CoordinatorNode >= 0);
//		Assert(innerPlan(stmt->planTree) == NULL);
		stmt->planTree = make_exchange(stmt->planTree,
				frOpts, false, false, node_number, nodes_at_cluster);
	}
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
	ProcessUtility_hook = HOOK_Utility_injection;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = HOOK_Parser_injection;

	EXCHANGE_Init_methods();

	PLAN_Hooks_init();
	EXEC_Hooks_init();

	CONN_Init_module();
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

Datum
set_query_id(PG_FUNCTION_ARGS)
{
	int port;
	CoordinatorNode = PG_GETARG_INT32(0);

	port = CONN_Init_socket(0);
	Assert(port >= 0);

	PG_RETURN_INT32(port);
}

Datum set_exchange_ports(PG_FUNCTION_ARGS)
{
	/* Parse input string to exchange node ports */
	char	*ports = TextDatumGetCString(PG_GETARG_DATUM(0));

	CONN_Parse_ports(ports);
	PG_RETURN_VOID();
}

Datum
isLocalValue(PG_FUNCTION_ARGS)
{
	char			*relname = TextDatumGetCString(PG_GETARG_DATUM(0));
	int				value = PG_GETARG_INT32(1);
	bool			result;
	fr_options_t	frOpts;
	fragmentation_fn_t	func;
//elog(LOG, "isLocalValue: %s, %d", relname, value);
	if (nfrRelations == 0)
		load_description_frag();
	if (nfrRelations == 0)
		PG_RETURN_BOOL(true);

	frOpts = getRelFrag(relname);
	func = frFuncs(frOpts.funcId);

	result = (func(value, node_number, nodes_at_cluster) == node_number);

	PG_RETURN_BOOL(result);
}

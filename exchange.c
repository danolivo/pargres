#include "postgres.h"

#include "common.h"
#include "connection.h"
#include "exchange.h"
#include "nodes/makefuncs.h"
#include "pargres.h"

static CustomScanMethods	exchange_plan_methods;
static CustomExecMethods	exchange_exec_methods;

static void EXCHANGE_Begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *EXCHANGE_Execute(CustomScanState *node);
static void EXCHANGE_End(CustomScanState *node);
static void EXCHANGE_Rescan(CustomScanState *node);
static void EXCHANGE_Explain(CustomScanState *node, List *ancestors, ExplainState *es);
static Node *EXCHANGE_Create_state(CustomScan *node);

static int fragmentation_fn_default(int value, int nnodes, int mynum);
static int fragmentation_fn_gather(int value, int nodenum, int nnodes);


void
EXCHANGE_Init_methods(void)
{
	exchange_plan_methods.CustomName 			= "ExchangePlan";
	exchange_plan_methods.CreateCustomScanState	= EXCHANGE_Create_state;
	RegisterCustomScanMethods(&exchange_plan_methods);

	/* setup exec methods */
	exchange_exec_methods.CustomName				= "Exchange";
	exchange_exec_methods.BeginCustomScan			= EXCHANGE_Begin;
	exchange_exec_methods.ExecCustomScan			= EXCHANGE_Execute;
	exchange_exec_methods.EndCustomScan				= EXCHANGE_End;
	exchange_exec_methods.ReScanCustomScan			= EXCHANGE_Rescan;
	exchange_exec_methods.MarkPosCustomScan			= NULL;
	exchange_exec_methods.RestrPosCustomScan		= NULL;
	exchange_exec_methods.EstimateDSMCustomScan  	= NULL;
	exchange_exec_methods.InitializeDSMCustomScan 	= NULL;
	exchange_exec_methods.InitializeWorkerCustomScan= NULL;
	exchange_exec_methods.ReInitializeDSMCustomScan = NULL;
	exchange_exec_methods.ShutdownCustomScan		= NULL;
	exchange_exec_methods.ExplainCustomScan			= EXCHANGE_Explain;
}

static Node *
EXCHANGE_Create_state(CustomScan *node)
{
	ExchangeState *state;
	ListCell   *lc;
	Var		   *var;
	List	   *tlist = NIL;

	state = (ExchangeState *) palloc0(sizeof(ExchangeState));
	NodeSetTag(state, T_CustomScanState);

	tlist = node->scan.plan.targetlist;
	foreach(lc, node->scan.plan.lefttree->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		var = makeVarFromTargetEntry(OUTER_VAR, tle);
						tlist = lappend(tlist,
										makeTargetEntry((Expr *) var,
														tle->resno,
														NULL,
														tle->resjunk));
	}
	node->scan.plan.targetlist = tlist;

	state->css.flags = node->flags;
	state->css.methods = &exchange_exec_methods;

	/* Extract necessary variables */
	state->frOpts.attno = intVal(list_nth(node->custom_private, 4));
	state->frOpts.funcId = intVal(list_nth(node->custom_private, 5));
	state->broadcast_mode = intVal(list_nth(node->custom_private, 2));
	state->drop_duplicates = intVal(list_nth(node->custom_private, 3));
	state->mynode = intVal(list_nth(node->custom_private, 1));
	state->nnodes = intVal(list_nth(node->custom_private, 0));

	/* Add Pointer to private EXCHANGE data to the list */
	ExchangeNodesPrivate = lappend(ExchangeNodesPrivate, (Node *)state);

	/* There should be exactly one subplan */
//	Assert(list_length(node->custom_plans) == 1);
	Assert(!node->scan.plan.qual);

	return (Node *) state;
}

static void
EXCHANGE_Begin(CustomScanState *node, EState *estate, int eflags)
{
	ExchangeState	*state = (ExchangeState *) node;
	TupleDesc		tupDesc;

	outerPlanState(node) = ExecInitNode(outerPlan(node->ss.ps.plan), estate, eflags);

	Assert(innerPlan(node->ss.ps.plan) == NULL);

	tupDesc = ExecGetResultType(outerPlanState(node));
	node->ss.ss_ScanTupleSlot = ExecInitExtraTupleSlot(estate, tupDesc);
	node->ss.ps.ps_ResultTupleSlot = ExecInitExtraTupleSlot(estate, tupDesc);
//	ExecAssignProjectionInfo(&state->css.ss.ps, NULL);

	state->NetworkIsActive = true;
	state->LocalStorageIsActive = true;
	state->NetworkStorageTuple = 0;
	state->LocalStorageTuple = 0;

	state->read_sock = palloc(sizeof(pgsocket)*nodes_at_cluster);
	Assert(state->read_sock != NULL);
	state->write_sock = palloc(sizeof(pgsocket)*nodes_at_cluster);
	Assert(state->write_sock != NULL);
	CONN_Init_exchange(state->read_sock, state->write_sock);

//	elog(INFO, "EXCHA2-->");
}

static TupleTableSlot *
GetTupleFromNetwork(ExchangeState *state, TupleTableSlot *slot, bool *NetworkIsActive)
{
	int res;
	HeapTuple tuple;

	Assert(state->read_sock > 0);
	tuple = CONN_Recv(state->read_sock, &res);

	if (res < 0)
	{
//		elog(LOG, "Network off. res=%d", res);
		*NetworkIsActive = false;
		return ExecClearTuple(slot);
	}
	else if (res == 0)
	{
		return ExecClearTuple(slot);
	}
	else
	{
//		elog(LOG, "Tuple Received: t_len=%d", tuple->t_len);
		ExecStoreHeapTuple(tuple, slot, false);
		return slot;
	}
}

static TupleTableSlot *
EXCHANGE_Execute(CustomScanState *node)
{
	PlanState		*child_ps = outerPlanState(node);
	TupleTableSlot	*slot = node->ss.ss_ScanTupleSlot;
	bool			isnull;
	Datum			value;
	ExchangeState	*state = (ExchangeState *)node;
	int				destnode;

	for (;;)
	{
		fragmentation_fn_t	frfunc;
		int					val;

		if (state->NetworkIsActive)
		{
			slot = GetTupleFromNetwork(state, node->ss.ss_ScanTupleSlot, &state->NetworkIsActive);

			if (!TupIsNull(slot))
			{
				state->NetworkStorageTuple++;
				break;
			}
		}

		if (state->LocalStorageIsActive)
		{
			slot = ExecProcNode(child_ps);

			if (TupIsNull(slot))
			{
				elog(LOG, "Close all outcoming connections. CoordinatorNode=%d", CoordinatorNode);
				CONN_Exchange_close(state->write_sock);
//				elog(LOG, "Close LocalStorageIsActive: state->NetworkIsActive=%u", state->NetworkIsActive);
				state->LocalStorageIsActive = false;
			} else
				state->LocalStorageTuple++;
		}

		if (TupIsNull(slot))
		{
			if (!state->NetworkIsActive && !state->LocalStorageIsActive)
			{
				return slot;
			}
			else
				continue;
		}

		if (slot->tts_nvalid > 0)
			/*
			 * If slot contains virtual tuple (after aggregate, for example),
			 * we need to materialize it.
			 */
			ExecMaterializeSlot(slot);

		if (state->broadcast_mode)
		{
			int destnode;
			int tupsize = offsetof(HeapTupleData, t_data);

			for (destnode = 0; destnode < nodes_at_cluster; destnode++)
			{
				if (state->write_sock[destnode] == PGINVALID_SOCKET)
					continue;

				CONN_Send(state->write_sock[destnode], slot->tts_tuple, tupsize);
				CONN_Send(state->write_sock[destnode], slot->tts_tuple->t_data, slot->tts_tuple->t_len);
			}

			/* Send tuple to myself */
			break;
		}
		else if (state->frOpts.funcId == FR_FUNC_GATHER)
		{
			destnode = CoordinatorNode;
		}
		else
		{
			/* Extract value of cell in a distribution domain */
			value = slot_getattr(slot, state->frOpts.attno, &isnull);
			Assert(!isnull);

			frfunc = frFuncs(state->frOpts.funcId);
			val = DatumGetInt32(value);
//			elog(LOG, "mynode=%d value=%d", state->mynode, val);
			destnode = frfunc(val, state->mynode, state->nnodes);
		}

		if (destnode == state->mynode)
			break;
		else if (state->drop_duplicates)
		{
//			elog(LOG, "DROP Duplicates!");
			continue;
		}
		else
		{
			int tupsize = offsetof(HeapTupleData, t_data);
//			elog(LOG, "AGG result natts: %d tuple=%u", slot->tts_tupleDescriptor->natts, slot->tts_tuple == NULL);

//			elog(LOG, "Send Tuple!: tupsize=%d len=%d Oid=%d", tupsize, slot->tts_tuple->t_len, slot->tts_tuple->t_tableOid);
			Assert(state->write_sock[destnode] > 0);
			CONN_Send(state->write_sock[destnode], slot->tts_tuple, tupsize);
			CONN_Send(state->write_sock[destnode], slot->tts_tuple->t_data, slot->tts_tuple->t_len);
			/* ToDo: Send tuple to corresponding destnode exchange */
			continue;
		}
	}
//	elog(INFO, "EXCHA-->4");
	return slot;
}

static void
EXCHANGE_End(CustomScanState *node)
{
//	ExchangeState	*state = (ExchangeState *)node;

//	elog(LOG, "END Exchange: LocalStorageTuple=%d NetworkStorageTuple=%d, CoordinatorNode=%d", state->LocalStorageTuple, state->NetworkStorageTuple, CoordinatorNode);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecEndNode(outerPlanState(node));
}

static void
EXCHANGE_Rescan(CustomScanState *node)
{
//	elog(LOG, "I am in EXCHANGE_Rescan()!");
}

static void
EXCHANGE_Explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CustomScan			*cscan = (CustomScan *) node->ss.ps.plan;
	int					nnodes = intVal(list_nth(cscan->custom_private, 0));
	int					mynode = intVal(list_nth(cscan->custom_private, 1));
	bool				broadcast_mode = intVal(list_nth(cscan->custom_private, 2));
	bool				drop_duplicates = intVal(list_nth(cscan->custom_private, 3));
	fr_options_t		frOpts;
	StringInfoData		str;

	frOpts.attno = intVal(list_nth(cscan->custom_private, 4));
	frOpts.funcId = intVal(list_nth(cscan->custom_private, 5));
	initStringInfo(&str);
//	nnodes = list_nth(cscan->custom_private, 0);
	appendStringInfo(&str, "attno: %d, funcId: %d, mynode=%d, nnodes=%d, Drop duplicates: %u, bcast: %u",
					 frOpts.attno,
					 frOpts.funcId,
					 mynode,
					 nnodes,
					 drop_duplicates, broadcast_mode);

	ExplainPropertyText("Exchange node", str.data, es);
}

/*
 * --------------------------------
 *  Exchange implementation
 * --------------------------------
 */

Plan *
make_exchange(Plan *subplan, fr_options_t frOpts,
							bool drop_duplicates,
							bool broadcast_mode,
							int mynode, int nnodes)
{
	CustomScan			*node = makeNode(CustomScan);
	Plan				*plan = &node->scan.plan;

	/* Init plan by GATHER analogy */
	plan->initPlan = subplan->initPlan;
	subplan->initPlan = NIL;

	/* Copy costs etc */
	plan->startup_cost = subplan->startup_cost;
	plan->total_cost = subplan->total_cost;
	plan->plan_rows = subplan->plan_rows;
	plan->plan_width = subplan->plan_width;
	plan->qual = NIL;
	plan->lefttree = subplan;
	plan->righttree = NULL;
	plan->parallel_aware = false;
	plan->parallel_safe = false;
	plan->targetlist = NULL;

	/* Setup methods and child plan */
	node->methods = &exchange_plan_methods;
//	node->custom_plans = list_make1(subplan);
	node->custom_scan_tlist = NIL;
	node->scan.scanrelid = 0;
	node->custom_plans = NIL;
	node->custom_exprs = NIL;
	node->custom_private = NIL;

	/* Pack private info */
	node->custom_private = lappend(node->custom_private, makeInteger(nnodes));
	node->custom_private = lappend(node->custom_private, makeInteger(mynode));
	node->custom_private = lappend(node->custom_private, makeInteger(broadcast_mode));
	node->custom_private = lappend(node->custom_private, makeInteger(drop_duplicates));
	node->custom_private = lappend(node->custom_private, makeInteger(frOpts.attno));
	node->custom_private = lappend(node->custom_private, makeInteger(frOpts.funcId));

	return plan;
}

static int
fragmentation_fn_default(int value, int mynum, int nnodes)
{
//	elog(LOG, "value: %d nnodes: %d", value, nnodes);
	return value%nnodes;
}

static int
fragmentation_fn_gather(int value, int nodenum, int nnodes)
{
	Assert((nodenum >= 0) && (nodenum < nnodes));
	Assert(CoordinatorNode >= 0);

	return CoordinatorNode;
}

static int
fragmentation_fn_empty(int value, int nnodes, int mynum)
{
	return mynum;
}

fragmentation_fn_t
frFuncs(fr_func_id fid)
{
	switch (fid)
	{
	case FR_FUNC_DEFAULT:
		return fragmentation_fn_default;
	case FR_FUNC_GATHER:
		return fragmentation_fn_gather;

	case FR_FUNC_NINITIALIZED:
		return fragmentation_fn_empty;
	default:
		elog(ERROR, "Undefined function");
	}
	return NULL;
}

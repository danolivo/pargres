#include "postgres.h"

#include "common.h"
#include "connection.h"
#include "exchange.h"
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
	ExchangePrivateData	*Data;
	ExchangeState *state;

//	node->custom_scan_tlist = node->scan.plan.targetlist;
	state = (ExchangeState *) palloc0(sizeof(ExchangeState));
	NodeSetTag(state, T_CustomScanState);
	Data = (ExchangePrivateData *) list_nth(node->custom_private, 0);

	state->css.flags = node->flags;
	state->css.methods = &exchange_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);
	state->frOpts = Data->frOpts;
	state->drop_duplicates = Data->drop_duplicates;
	state->mynode = Data->mynode;
	state->nnodes = Data->nnodes;

	state->read_sock = palloc(sizeof(pgsocket)*nodes_at_cluster);
	state->write_sock = palloc(sizeof(pgsocket)*nodes_at_cluster);
	CONN_Init_exchange(state->read_sock, state->write_sock);

	/* Add Pointer to private EXCHANGE data to the list */
	ExchangeNodesPrivate = lappend(ExchangeNodesPrivate, (Node *)state);

	/* There should be exactly one subplan */
	Assert(list_length(node->custom_plans) == 1);
	Assert(!node->scan.plan.qual);

	return (Node *) state;
}

static void
EXCHANGE_Begin(CustomScanState *node, EState *estate, int eflags)
{
	ExchangeState	*state = (ExchangeState *) node;
	PlanState		*child_ps;
	TupleDesc		tupDesc;

	/* It's convenient to store PlanState in 'custom_ps' */
	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));
	child_ps = (PlanState *) linitial(node->custom_ps);
	node->ss.ps.plan->targetlist = child_ps->plan->targetlist;
	outerPlanState(node) = child_ps;
	tupDesc = ExecGetResultType(outerPlanState(node));
	Assert(innerPlan(node->ss.ps.plan) == NULL);
//	if (!node->ss.ps.plan->targetlist)
//	{
//		node->ss.ps.ps_ResultTupleSlot = ExecInitExtraTupleSlot(estate, tupDesc);
//	elog(LOG, "EXCH before natts= %d %d", node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->natts, node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts);
//	elog(LOG, "EXCH list_length=%d", list_length(node->ss.ps.plan->targetlist));
	node->ss.ss_ScanTupleSlot = ExecInitExtraTupleSlot(estate, tupDesc);
	node->ss.ps.ps_ResultTupleSlot = ExecInitExtraTupleSlot(estate, tupDesc);
//	ExecInitResultTupleSlotTL(estate, &node->ss.ps);
//	ExecConditionalAssignProjectionInfo(&node->ss.ps, tupDesc, OUTER_VAR);
//		elog(LOG, "Children natts=%d", child_ps->ps_ResultTupleSlot->tts_tupleDescriptor->natts);
//	}

//
//	ExecInitResultTupleSlotTL(child_ps->state, &node->ss.ps);
//	ExecInitScanTupleSlot(child_ps->state, &node->ss, child_ps->ps_ResultTupleSlot->tts_tupleDescriptor);

	//	node->ss.ps.ps_ResultTupleSlot = MakeTupleTableSlot(child_ps->ps_ResultTupleSlot->tts_tupleDescriptor);
//	elog(LOG, "EXCH Begin: natts=%d %d", node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->natts, node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts);
	state->NetworkIsActive = true;
	state->LocalStorageIsActive = true;
	state->NetworkStorageTuple = 0;
	state->LocalStorageTuple = 0;
}

//char tbuf[8192];

static TupleTableSlot *
GetTupleFromNetwork(ExchangeState *state, TupleTableSlot *slot, bool *NetworkIsActive)
{
	int res;
	HeapTuple tuple;

	tuple = CONN_Recv(state->read_sock, &res);

	if (res < 0)
	{
//		elog(LOG, "Network off");
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
	PlanState		*child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot	*slot = node->ss.ss_ScanTupleSlot;
	bool			isnull;
	Datum			value;
	ExchangeState	*state = (ExchangeState *)node;
	int				destnode;
	ExprContext		*econtext;

	econtext = state->css.ss.ps.ps_ExprContext;
	ResetExprContext(econtext);

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
//				elog(LOG, "Close all outcoming connections. CoordinatorNode=%d", CoordinatorNode);
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
				return NULL;
			}
			else
				continue;
		}

		/* Extract value of cell in a distribution domain */
		value = slot_getattr(slot, state->frOpts.attno, &isnull);
		Assert(!isnull);

		frfunc = frFuncs(state->frOpts.funcId);

		val = DatumGetInt32(value);

		destnode = frfunc(val, state->mynode, state->nnodes);
//		elog(LOG, "AFTER Analysis! destnode=%d state->mynode=%d", destnode, state->mynode);
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
//			elog(LOG, "Send Tuple!: tupsize=%d len=%d Oid=%d", tupsize, slot->tts_tuple->t_len, slot->tts_tuple->t_tableOid);
			CONN_Send(state->write_sock[destnode], slot->tts_tuple, tupsize);
			CONN_Send(state->write_sock[destnode], slot->tts_tuple->t_data, slot->tts_tuple->t_len);
			/* ToDo: Send tuple to corresponding destnode exchange */
			continue;
		}
	}

	return slot;
}

static void
EXCHANGE_End(CustomScanState *node)
{
//	ExchangeState	*state = (ExchangeState *)node;

//	elog(LOG, "END Exchange: LocalStorageTuple=%d NetworkStorageTuple=%d", state->LocalStorageTuple, state->NetworkStorageTuple);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecEndNode(linitial(node->custom_ps));
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
	ExchangePrivateData	*Data;
	StringInfoData		str;

	initStringInfo(&str);
	Data = (ExchangePrivateData *) list_nth(cscan->custom_private, 0);
	appendStringInfo(&str, "attno: %d, funcId: %d, DD: %u", Data->frOpts.attno,
													Data->frOpts.funcId,
													Data->drop_duplicates);

	ExplainPropertyText("Exchange node", str.data, es);
}

/*
 * --------------------------------
 *  Exchange implementation
 * --------------------------------
 */

Plan *
make_exchange(Plan *subplan, fr_options_t frOpts, bool drop_duplicates, int mynode, int nnodes)
{
	CustomScan			*node = makeNode(CustomScan);
	Plan				*plan = &node->scan.plan;
	ExchangePrivateData	*Data;

	/* Copy costs etc */
	plan->startup_cost = subplan->startup_cost;
	plan->total_cost = subplan->total_cost;
	plan->plan_rows = subplan->plan_rows;
	plan->plan_width = subplan->plan_width;

	/* Setup methods and child plan */
	node->methods = &exchange_plan_methods;
	node->custom_plans = list_make1(subplan);

	/* Build an appropriate target list using a cached Relation entry */
	plan->targetlist = NULL;
	node->custom_scan_tlist = NULL;

	/* Init plan by GATHER analogy */
//	plan->initPlan = subplan->initPlan;
//	subplan->initPlan = NIL;
	plan->qual = NIL;
	plan->lefttree = subplan;
	plan->righttree = NULL;
	plan->parallel_aware = false;
	plan->parallel_safe = false;

	/* No physical relation will be scanned */
	node->scan.scanrelid = 0;

	/* Pack private info */
	Data = palloc(sizeof(ExchangePrivateData));
	Data->frOpts = frOpts;
	Data->drop_duplicates = drop_duplicates;
	Data->mynode = mynode;
	Data->nnodes = nnodes;
//	elog(LOG, "make_exchange: %d %d", mynode, nnodes);
	node->custom_private = lappend(NIL, Data);

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

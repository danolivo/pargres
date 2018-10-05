#include "postgres.h"

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
static fragmentation_fn_t frFuncs(fr_func_id fid);


static Node *
EXCHANGE_Create_state(CustomScan *node)
{
	ExchangePrivateData	*Data;
	ExchangeState *state;

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
//	elog(LOG, "EXCHANGE_Create_state: %d %d", state->mynode, state->nnodes);
	/* There should be exactly one subplan */
	Assert(list_length(node->custom_plans) == 1);

	return (Node *) state;
}

static void
EXCHANGE_Begin(CustomScanState *node, EState *estate, int eflags)
{
	ExchangeState	   *state = (ExchangeState *) node;

	/* It's convenient to store PlanState in 'custom_ps' */
	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));
}

static TupleTableSlot *
EXCHANGE_Execute(CustomScanState *node)
{
	PlanState		*child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot	*slot = NULL;
	bool			isnull;
	Datum			value;
	ExchangeState	*state = (ExchangeState *)node;
	int				destnode;

//	elog(LOG, "EXCHANGE_Execute: %d %d %u %d %d", state->frOpts.attno, state->frOpts.funcId, state->drop_duplicates,
//			state->mynode, state->nnodes);
	for (;;)
	{
		fragmentation_fn_t frfunc;
		int val;

		slot = ExecProcNode(child_ps);

		if (TupIsNull(slot))
			return NULL;

		/* Extract value of cell in a distribution domain */
		value = slot_getattr(slot, state->frOpts.attno, &isnull);
		Assert(!isnull);

		frfunc = frFuncs(state->frOpts.funcId);

		val = DatumGetInt32(value);

		destnode = frfunc(val, state->mynode, state->nnodes);

		if (destnode == state->mynode)
			break;
		else if (state->drop_duplicates)
			continue;
		else
			/* ToDo: Send tuple to corresponding destnode exchange */
			continue;
	}
	return slot;
}

static void
EXCHANGE_End(CustomScanState *node)
{
//	elog(LOG, "I am in EXCHANGE_End()!");
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
	plan->targetlist = subplan->targetlist;
//	RelationClose(parent_rel);

	/* No physical relation will be scanned */
	node->scan.scanrelid = 0;

	node->custom_scan_tlist = NULL;

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

void
EXCHANGE_Init(void)
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

static int
fragmentation_fn_default(int value, int mynum, int nnodes)
{
	elog(LOG, "value: %d nnodes: %d", value, nnodes);
	return value%nnodes;
}

static int
fragmentation_fn_empty(int value, int nnodes, int mynum)
{
	return mynum;
}

static fragmentation_fn_t
frFuncs(fr_func_id fid)
{
	switch (fid)
	{
	case FR_FUNC_DEFAULT:
		return fragmentation_fn_default;

	default:
		elog(LOG, "Undefined function");
		return fragmentation_fn_empty;
	}
}

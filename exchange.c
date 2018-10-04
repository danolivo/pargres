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
	PlanState	*child_ps = (PlanState *) linitial(node->custom_ps);

	return ExecProcNode(child_ps);
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
	CustomScan		*cscan = (CustomScan *) node->ss.ps.plan;
	fr_options_t	*frOpts;
	StringInfoData	str;

	initStringInfo(&str);
	frOpts = (fr_options_t *) list_nth(cscan->custom_private, 0);
	appendStringInfo(&str, "attno: %d", frOpts->attno);

	ExplainPropertyText("Exchange node", str.data, es);
}

static Node *
EXCHANGE_Create_state(CustomScan *node)
{
	ExchangeState *state;

	state = (ExchangeState *) palloc0(sizeof(ExchangeState));
	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &exchange_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);

	/* There should be exactly one subplan */
	Assert(list_length(node->custom_plans) == 1);

	return (Node *) state;
}

/*
 * --------------------------------
 *  Exchange implementation
 * --------------------------------
 */

Plan *
make_exchange(Plan *subplan, fr_options_t frOpts)
{
	CustomScan *cscan = makeNode(CustomScan);
	fr_options_t *privateFrOpts = palloc(sizeof(fr_options_t));

	/* Copy costs etc */
	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	/* Setup methods and child plan */
	cscan->methods = &exchange_plan_methods;
	cscan->custom_plans = list_make1(subplan);

	/* Build an appropriate target list using a cached Relation entry */
	cscan->scan.plan.targetlist = subplan->targetlist;
//	RelationClose(parent_rel);

	/* No physical relation will be scanned */
	cscan->scan.scanrelid = 0;

	cscan->custom_scan_tlist = NULL;

	/* Pack private info */
	*privateFrOpts = frOpts;
	cscan->custom_private = lappend(NIL, privateFrOpts);

	return &cscan->scan.plan;
}

void
EXCHANGE_Hooks(void)
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

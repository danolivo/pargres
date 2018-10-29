#include "postgres.h"

#include "common.h"
#include "connection.h"
#include "exchange.h"
#include "nodes/makefuncs.h"
#include "pargres.h"

#include "unistd.h"


static CustomScanMethods	exchange_plan_methods;
static CustomExecMethods	exchange_exec_methods;

static void EXCHANGE_Begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *EXCHANGE_Execute(CustomScanState *node);
static void EXCHANGE_End(CustomScanState *node);
static void EXCHANGE_Rescan(CustomScanState *node);
static void EXCHANGE_ReInitializeDSM(CustomScanState *node, ParallelContext *pcxt,
		  	  	  	  	 void *coordinate);
static void EXCHANGE_Explain(CustomScanState *node, List *ancestors,
							 ExplainState *es);
static Size EXCHANGE_EstimateDSM(CustomScanState *node, ParallelContext *pcxt);
static void EXCHANGE_InitializeDSM(CustomScanState *node, ParallelContext *pcxt,
								   void *coordinate);
static void EXCHANGE_InitializeWorker(CustomScanState *node,
									  shm_toc *toc,
									  void *coordinate);
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
	exchange_exec_methods.EstimateDSMCustomScan  	= EXCHANGE_EstimateDSM;
	exchange_exec_methods.InitializeDSMCustomScan 	= EXCHANGE_InitializeDSM;
	exchange_exec_methods.InitializeWorkerCustomScan= EXCHANGE_InitializeWorker;
	exchange_exec_methods.ReInitializeDSMCustomScan = EXCHANGE_ReInitializeDSM;
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
	state->connPool = NULL;
	state->conn.rsock = NULL;
	state->conn.wsock = NULL;

	Assert(!node->scan.plan.qual);
	return (Node *) state;
}

static int number=0;

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

	state->NetworkIsActive = true;
	state->LocalStorageIsActive = true;
	state->NetworkStorageTuple = 0;
	state->LocalStorageTuple = 0;
	state->number = number++;

	/* Need to establish connection on the first call */
	Assert(!state->conn.rsock);
	Assert(!state->conn.wsock);

	if (!PargresInitialized)
		/* EXCHANGE_Begin: escape for the worker initialization */
		return;

	if (!BackendConnInfo)
	{
		if (!state->connPool)
		{
			/* If this plan not executed by workers, we need to create
			 * connection pool with one ConnInfo data before initialize read and
			 * write sockets.
			 * Otherwise, the connection pool was created by the
			 * ExecParallelInitializeDSM () function earlier.
			 */
			state->connPool = palloc(sizeof(ConnInfoPool));
			CreateConnectionPool(state->connPool, 1, state->nnodes, state->mynode);
		}
		BackendConnInfo = GetConnInfo(state->connPool);
	}

	CONN_Init_exchange(BackendConnInfo , &state->conn, state->mynode, state->nnodes);
}

static TupleTableSlot *
GetTupleFromNetwork(ExchangeState *state, TupleTableSlot *slot, bool *NetworkIsActive)
{
	int res;
	HeapTuple tuple;

	Assert(state->conn.rsock > 0);
	tuple = CONN_Recv_any(state->conn.rsock, state->conn.rsIsOpened, &res);

	if (res < 0)
	{
		*NetworkIsActive = false;
		return ExecClearTuple(slot);
	}
	else if (res == 0)
	{
		return ExecClearTuple(slot);
	}
	else
	{
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
				elog(LOG, "Launch CONN_Exchange_close, num=%d", state->number);
				CONN_Exchange_close(&state->conn);
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
				if (state->conn.wsock[destnode] == PGINVALID_SOCKET)
					continue;

				CONN_Send(state->conn.wsock[destnode], slot->tts_tuple, tupsize);
				CONN_Send(state->conn.wsock[destnode], slot->tts_tuple->t_data, slot->tts_tuple->t_len);
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

			destnode = frfunc(val, state->mynode, state->nnodes);
		}

		if (destnode == state->mynode)
			break;
		else if (state->drop_duplicates)
			continue;
		else
		{
			int tupsize = offsetof(HeapTupleData, t_data);

			Assert(state->conn.wsock[destnode] > 0);
			CONN_Send(state->conn.wsock[destnode], slot->tts_tuple, tupsize);
			CONN_Send(state->conn.wsock[destnode], slot->tts_tuple->t_data, slot->tts_tuple->t_len);
			continue;
		}
	}
	return slot;
}

static void
EXCHANGE_End(CustomScanState *node)
{
	ExchangeState	*state = (ExchangeState *)node;
	int i;

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	Assert(state->conn.rsock);
	Assert(state->conn.wsock);

	for (i = 0; i < nodes_at_cluster; i++)
	{
		if (i == node_number)
			continue;

		Assert(state->conn.rsock[i] != PGINVALID_SOCKET);
		Assert(state->conn.wsock[i] != PGINVALID_SOCKET);
		if (state->conn.rsIsOpened[i] != false)
			elog(LOG,
			"Read socket %d to node %d is not closed. It is explain query?",
			state->conn.rsock[i], i);

		closesocket(state->conn.rsock[i]);
		state->conn.rsock[i] = PGINVALID_SOCKET;

		if (state->conn.wsIsOpened[i] != false)
			elog(LOG,
			"Write socket %d to node %d is not closed. It is explain query?",
			state->conn.wsock[i], i);

		closesocket(state->conn.wsock[i]);
		state->conn.wsock[i] = PGINVALID_SOCKET;
	}
	ExecEndNode(outerPlanState(node));
}

static void
EXCHANGE_Rescan(CustomScanState *node)
{
	PlanState		*outerPlan = outerPlanState(node);
	ExchangeState	*state = (ExchangeState *)node;
	int				i;

	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);

	Assert(state->conn.rsock != NULL);
	Assert(state->conn.wsock != NULL);

	for (i = 0; i < nodes_at_cluster; i++)
	{
		if (i == node_number)
			continue;

		state->conn.rsIsOpened[i] = true;
		state->conn.wsIsOpened[i] = true;
	}
}

static void
EXCHANGE_ReInitializeDSM(CustomScanState *node, ParallelContext *pcxt,
		  	  	  	  	 void *coordinate)
{
	/* ToDo */
	elog(LOG, "I am in ReInitializeDSM()!");
	Assert(0);
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
	plan->parallel_aware = true; /* Use Shared Memory in parallel worker */
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

static Size
EXCHANGE_EstimateDSM(CustomScanState *node, ParallelContext *pcxt)
{
	return sizeof(ConnInfoPool) * pcxt->nworkers;
}

static void
EXCHANGE_InitializeDSM(CustomScanState *node, ParallelContext *pcxt,
					   void *coordinate)
{
	/*
	 * coordinate - pointer to shared memory segment.
	 * node->pscan_len - size of the coordinate - is defined by
	 * EstimateDSMCustomScan() function.
	 */
	if (ProcessSharedConnInfoPool.size < 0)
		CreateConnectionPool(&ProcessSharedConnInfoPool, pcxt->nworkers, nodes_at_cluster, node_number);

	memcpy(coordinate, &ProcessSharedConnInfoPool, sizeof(ConnInfoPool));
}

static void
EXCHANGE_InitializeWorker(CustomScanState *node,
						  shm_toc *toc,
						  void *coordinate)
{
	ExchangeState	*state = (ExchangeState *) node;

	state->connPool = (ConnInfoPool *) coordinate;
	CoordinatorNode = state->connPool->CoordinatorNode;
	PargresInitialized = true;

	Assert(!state->conn.rsock);
	Assert(!state->conn.wsock);

	if (!BackendConnInfo)
		BackendConnInfo = GetConnInfo(state->connPool);

	elog(LOG, "EXCHANGE_InitializeWorker: mynode=%d nnodes=%d", state->mynode, state->nnodes);
	CONN_Init_exchange(BackendConnInfo , &state->conn, state->mynode, state->nnodes);
}

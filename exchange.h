/*
 * exchange.h
 *
 *  Created on: 3 окт. 2018 г.
 *      Author: andrey
 */

#ifndef EXCHANGE_H_
#define EXCHANGE_H_

#include "commands/explain.h"
#include "nodes/extensible.h"
#include "optimizer/planner.h"

typedef int (*fragmentation_fn_t) (int value, int mynum, int nnodes);

typedef enum
{
	FR_FUNC_NINITIALIZED = 0,
	FR_FUNC_DEFAULT,
	FR_FUNC_GATHER
} fr_func_id;

typedef struct
{
	int			attno;
	fr_func_id	funcId;
} fr_options_t;

typedef struct
{
	fr_options_t	frOpts;
	bool			drop_duplicates;
	int				mynode;
	int				nnodes;
} ExchangePrivateData;

typedef struct
{
	CustomScanState	css;
	Plan			*subplan;
	fr_options_t	frOpts;
	bool			drop_duplicates;
	int				mynode;
	int				nnodes;
} ExchangeState;

extern void EXCHANGE_Init(void);
extern Plan *make_exchange(Plan *subplan, fr_options_t frOpts, bool drop_duplicates, int mynode, int nnodes);

#endif /* EXCHANGE_H_ */

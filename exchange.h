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

typedef int (*fragmentation_fn_t) (int value, int nnodes);

typedef enum
{
	FR_FUNC_DEFAULT = 0
} fr_func_id;

typedef struct
{
	int			attno;
	fr_func_id	funcId;
} fr_options_t;

typedef struct
{
	CustomScanState		css;
	Plan				*subplan;
} ExchangeState;

extern void EXCHANGE_Hooks(void);
extern Plan *make_exchange(Plan *subplan, fr_options_t frOpts);

#endif /* EXCHANGE_H_ */

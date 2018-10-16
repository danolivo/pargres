/*
 * common.c
 *
 *      Author: andrey
 */

#include "postgres.h"

#include "common.h"

int CoordinatorNode = -1;

List *ExchangeNodesPrivate = NULL;

bool PargresInitialized = false;

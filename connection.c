/*
 * connection.c
 *
 *  Created on: 8 окт. 2018 г.
 *      Author: andrey
 */

#include "postgres.h"

#include "common/ip.h"
#include "libpq/libpq.h"
#include "libpq-fe.h"
#include "utils/memutils.h"

#include "common.h"
#include "connection.h"

#include "stdio.h"

typedef PGconn* ppgconn;

MemoryContext	ParGRES_context;

/*
 * GUC: node_number
 */
int node_number;
int nodes_at_cluster;
const int EXCHANGE_BASE_PORT = 6543;

static int BACKEND_PORT = -1;
#define STRING_SIZE_MAX	(1024)
static ppgconn *conn = NULL;

NON_EXEC_STATIC pgsocket ExchangeSock = PGINVALID_SOCKET;

void
CONN_Init_module(void)
{
	ParGRES_context = AllocSetContextCreate(TopMemoryContext,
												"PargresMemoryContext",
												ALLOCSET_DEFAULT_SIZES);
}

/*
 * Use system-based choice of free port.
 */
int
CONN_Init_socket(void)
{
	struct sockaddr_in	addr;
	socklen_t					addrlen = sizeof(addr);

	addr.sin_family = AF_INET;
	addr.sin_port = 0;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);

	if (ExchangeSock != PGINVALID_SOCKET)
		return BACKEND_PORT;

	/* Setup socket for initial connections */
	ExchangeSock = socket(addr.sin_family, SOCK_STREAM, 0);
	Assert(ExchangeSock != PGINVALID_SOCKET);
	if (bind(ExchangeSock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
		perror("bind");

	listen(ExchangeSock, 1024);

	getsockname(ExchangeSock, (struct sockaddr *)&addr, &addrlen);
	BACKEND_PORT = addr.sin_port;

	return BACKEND_PORT;
}

/*
 * Initialize connections to all another instances
 */
int
CONN_Set_all(void)
{
	int node;
	char conninfo[STRING_SIZE_MAX];

	if (conn)
		return 1;
	conn = MemoryContextAllocZero(ParGRES_context, sizeof(ppgconn)*nodes_at_cluster);

	for (node = 0; node < nodes_at_cluster; node++)
	{
		if (node == node_number)
			conn[node] = NULL;
		else
		{
			sprintf(conninfo, "host=%s port=%d%c", "localhost", 5433+node, '\0');
			conn[node] = PQconnectdb(conninfo);
			if (PQstatus(conn[node]) == CONNECTION_BAD)
			{
				elog(LOG, "Connection error. conninfo: %s", conninfo);
				return -1;
			}
			else
				elog(LOG, "Connection established!: conninfo=%s", conninfo);
			//Assert(PQstatus(conn[node]) != CONNECTION_BAD);
		}
	}
	return 0;
}

int
CONN_Init_execution(int tag)
{
	int node;
	char command[STRING_SIZE_MAX];

	Assert(conn != NULL);

	sprintf(command, "SELECT set_query_id(%d, %d);%c", node_number, tag, '\0');
	elog(LOG, "CONN_Init_channel: %s", command);
	for (node = 0; node < nodes_at_cluster; node++)
	{
		PGresult *result;

		if (node == node_number)
			continue;
		elog(LOG, "CONN_Init_channel: %s node=%d", command, node);
		Assert(conn[node] != NULL);
		elog(LOG, "CONN Status: %d", PQstatus(conn[node]));
		result = PQexec(conn[node], command);
		Assert(PQresultStatus(result) != PGRES_FATAL_ERROR);
	}
	elog(LOG, "EOF CONN_Init_channel: %s", command);
	return 0;
}

int
CONN_Launch_query(const char *query)
{
	int node;

	for (node = 0; node < nodes_at_cluster; node++)
	{
		PGresult *result;

		if (node == node_number)
			continue;

		result = PQexec(conn[node], query);
		Assert(PQresultStatus(result) != PGRES_FATAL_ERROR);
	}

	return 0;
}

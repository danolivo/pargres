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
#include "unistd.h"

typedef PGconn* ppgconn;

MemoryContext	ParGRES_context;

/*
 * GUC: node_number
 */
int node_number;
int nodes_at_cluster;

static int ExchangePort[NODES_MAX_NUM];
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
CONN_Init_socket(int port)
{
	struct sockaddr_in	addr;
	socklen_t					addrlen = sizeof(addr);

	addr.sin_family = AF_INET;
	addr.sin_port = (in_port_t) port;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);

	if (ExchangeSock != PGINVALID_SOCKET)
		return ExchangePort[node_number];

	/* Setup socket for initial connections */
	ExchangeSock = socket(addr.sin_family, SOCK_STREAM, 0);
	Assert(ExchangeSock != PGINVALID_SOCKET);
	if (bind(ExchangeSock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
	{
		perror("bind");
		return -1;
	}

	if (listen(ExchangeSock, 1024) != 0)
	{
		perror("listen");
		return -1;
	}

	getsockname(ExchangeSock, (struct sockaddr *)&addr, &addrlen);
	ExchangePort[node_number] = htons(addr.sin_port);
//	elog(LOG, "My addr: %d %lu", ExchangePort[node_number], htonl(addr.sin_addr.s_addr));
	Assert(ExchangePort[node_number] >= 0);
//	elog(LOG, "Init listen: %d, node=%d", ExchangePort[node_number], node_number);

	if (!pg_set_noblock(ExchangeSock))
		elog(ERROR, "Nonblocking socket failed. ");

	return ExchangePort[node_number];
}

/*
 * Initialize connections to all another instances
 */
int
CONN_Set_all(void)
{
	int node;
	char conninfo[STRING_SIZE_MAX];

	if (!conn)
		/* Also, which set conn[node] to NULL value*/
		conn = MemoryContextAllocZero(ParGRES_context, sizeof(ppgconn)*nodes_at_cluster);

	for (node = 0; node < nodes_at_cluster; node++)
	{
		if ((node != node_number) && (conn[node] == NULL))
		{
			sprintf(conninfo, "host=%s port=%d%c", "localhost", 5433+node, '\0');
			conn[node] = PQconnectdb(conninfo);
			if (PQstatus(conn[node]) == CONNECTION_BAD)
			{
				elog(LOG, "Connection error. conninfo: %s", conninfo);
				return -1;
			}
			else
				elog(LOG, "Connection established!: conninfo=%s, status=%d", conninfo, PQstatus(conn[node]));
		}
	}
	return 0;
}

int
CONN_Init_execution(void)
{
	int node;
	char command[STRING_SIZE_MAX];
	char ports[STRING_SIZE_MAX] = "";

	Assert(conn != NULL);

	sprintf(command, "SELECT set_query_id(%d);%c", node_number, '\0');

	for (node = 0; node < nodes_at_cluster; node++)
	{
		PGresult *result;

		if (node == node_number)
			continue;

		Assert(conn[node] != NULL);

		result = PQexec(conn[node], command);

		Assert(PQresultStatus(result) != PGRES_FATAL_ERROR);
		Assert(PQntuples(result) == 1);
		sscanf(PQgetvalue(result, 0 ,0), "%d", &ExchangePort[node]);
	}

	/* Generate string with ports number */
	for (node = 0; node < nodes_at_cluster; node++)
	{
		char port[10];

		sprintf(port, "%d ", ExchangePort[node]);
		strcat(ports, port);
	}

	/* Send port numbers to all another nodes */
	sprintf(command, "SELECT set_exchange_ports('%s');%c", ports, '\0');

	for (node = 0; node < nodes_at_cluster; node++)
	{
		PGresult *result;

		if (node == node_number)
			continue;

		result = PQexec(conn[node], command);
		Assert(PQresultStatus(result) != PGRES_FATAL_ERROR);
	}

	return 0;
}

int
CONN_Launch_query(const char *query)
{
	int node;

	for (node = 0; node < nodes_at_cluster; node++)
	{
		int	result;

		if (node == node_number)
			continue;

		result = PQsendQuery(conn[node], query);

		if (result == 0)
			elog(ERROR, "Query sending error: %s", PQerrorMessage(conn[node]));

		Assert(result >= 0);
	}

	return 0;
}

void
CONN_Check_query_result(void)
{
	int node;
	PGresult *result;

	if (!conn)
		return;

	do
	{
		for (node = 0; node < nodes_at_cluster; node++)
		{
			if (conn[node] == NULL)
				continue;

			if ((result = PQgetResult(conn[node])) != NULL)
				break;
		}
	} while (result);
}

static int
sendall(int s, char *buf, int len, int flags)
{
    int total = 0;
    int n;

    while(total < len)
    {
        n = send(s, buf+total, len-total, flags);
        if(n == -1) { break; }
        total += n;
    }

    return (n==-1 ? -1 : total);
}

void
CONN_Init_exchange(pgsocket *read_sock, pgsocket *write_sock)
{
	int					node;
	struct sockaddr_in	addr;
	fd_set				readset;
	int					res;
	int					incoming = nodes_at_cluster - 1;
	int					messages = nodes_at_cluster - 1;
	pgsocket			*incoming_sock = palloc(sizeof(pgsocket)*nodes_at_cluster);

	Assert(read_sock > 0);

	/* Init sockets for connection for foreign servers */
	for (node = 0; node < nodes_at_cluster; node++)
	{
		if (node == node_number)
		{
			write_sock[node] = PGINVALID_SOCKET;
			read_sock[node] = PGINVALID_SOCKET;
			continue;
		}

		write_sock[node] = socket(AF_INET, SOCK_STREAM, 0);
		if(write_sock[node] < 0)
			perror("socket");

		addr.sin_family = AF_INET;
		addr.sin_port = htons(ExchangePort[node]);
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		res = connect(write_sock[node], (struct sockaddr *)&addr, sizeof(addr));
		if (res < 0)
			perror("CONNECT");
	}

	for (; (incoming > 0); )
	{
		FD_ZERO(&readset);
		FD_SET(ExchangeSock, &readset);

		if(select(ExchangeSock+1, &readset, NULL, NULL, NULL) <= 0)
			perror("select");

		Assert(FD_ISSET(ExchangeSock, &readset));

		incoming--;
		incoming_sock[incoming] = accept(ExchangeSock, NULL, NULL);
		if (incoming_sock[incoming] < 0)
		{
			if (errno != EWOULDBLOCK)
			{
				perror("  accept() failed");
			}

		}
		if (!pg_set_noblock(incoming_sock[incoming]))
			elog(ERROR, "Nonblocking socket failed. ");
	}




	for (node = 0; node < nodes_at_cluster; node++)
	{
		int buf = node_number;

		if (node == node_number)
			continue;

		sendall(write_sock[node], (char *)&buf, sizeof(int), 0);
	}

	while (messages > 0)
	{
		int high_sock = 0;
		int conns;

		FD_ZERO(&readset);
		for (node = 0; node < nodes_at_cluster-1; node++)
		{
			FD_SET(incoming_sock[node], &readset);

			if (high_sock < incoming_sock[node])
				high_sock = incoming_sock[node];
		}
		if((conns = select(high_sock+1, &readset, NULL, NULL, NULL)) <= 0)
			perror("select");

		for (node = 0; node < nodes_at_cluster-1; node++)
		{
			int	nodeID = 0;
			int	bytes_read;

			if (FD_ISSET(incoming_sock[node], &readset))
			{
				bytes_read = recv(incoming_sock[node], &nodeID, sizeof(int), 0);
				if (bytes_read != sizeof(int))
					elog(LOG, "EE bytes_read=%d", bytes_read);
				Assert(bytes_read == sizeof(int));
				Assert((nodeID >= 0) && (nodeID <nodes_at_cluster));
				read_sock[nodeID] = incoming_sock[node];

				messages--;
			}
		}
	}

	pfree(incoming_sock);
}

void
CONN_Exchange_close(pgsocket *write_sock)
{
	int node;

	for (node = 0; node < nodes_at_cluster; node++)
	{
		if (write_sock[node] == PGINVALID_SOCKET)
			continue;

		closesocket(write_sock[node]);
		write_sock[node] = PGINVALID_SOCKET;
	}
//	elog(LOG, "Close exchange connections");
}

void
CONN_Parse_ports(char *ports)
{
	int node = 0;
	char *token = strtok(ports, " ");

	while (token != NULL)
	{
		 sscanf(token, "%d\n", &ExchangePort[node]);
		 token = strtok(NULL, " ");
//		 elog(LOG, "ExchangePort[%d]: %d", node, ExchangePort[node]);
		 node++;
	}

	Assert(node == nodes_at_cluster);
}

void
CONN_Send(pgsocket sock, void *buf, int size)
{
	if (sendall(sock, (char *)buf, size, 0) < 0)
		perror("SEND Error");
}

HeapTuple
CONN_Recv(pgsocket *socks, int *res)
{
	int				i;
	struct timeval	timeout;
	HeapTupleData	htHeader;
	HeapTuple		tuple;
	fd_set readset;
	int high_sock = 0;

	Assert(socks != NULL);
	Assert(res != NULL);

	timeout.tv_sec = 0;
	timeout.tv_usec = 0;

	/*
	 * In this cycle we wait one from events:
	 * 1. Tuple arrived, return it to the caller immediately.
	 * 2. high_sock == 0: all connections closed. Returns -1.
	 * 3. No messages received. Returns 0.
	 */
		FD_ZERO(&readset);

		for (i = 0; i < nodes_at_cluster; i++)
		{
			if (socks[i] == PGINVALID_SOCKET)
				continue;

			if (high_sock < socks[i])
				high_sock = socks[i];

			FD_SET(socks[i], &readset);
		}

		/* We have any open incoming connections? */
		if (high_sock == 0)
		{
			*res = -1;
			return NULL;
		}
//elog(LOG, "HighSock=%d", high_sock);
		if ((*res = select(high_sock+1, &readset, NULL, NULL, &timeout)) < 0)
			perror("SELECT ERROR");

		/* Search for socket triggered */
		for (i = 0; i < nodes_at_cluster; i++)
		{
			if (socks[i] == PGINVALID_SOCKET)
				continue;

			if (FD_ISSET(socks[i], &readset))
			{
//				elog(LOG, "Before receive tuple");
				if ((*res = recv(socks[i], &htHeader, offsetof(HeapTupleData, t_data), 0)) > 0)
				{
					int res1;
//					elog(LOG, "CONN_Recv HeapTuple received: size=%d t_len=%u Oid=%u", *res, htHeader.t_len, htHeader.t_tableOid);
					FD_ZERO(&readset);
					FD_SET(socks[i], &readset);
					if ((res1 = select(socks[i]+1, &readset, NULL, NULL, NULL)) <= 0)
						Assert(0);

					tuple = (HeapTuple) palloc0(HEAPTUPLESIZE + htHeader.t_len);
					memcpy(tuple, &htHeader, HEAPTUPLESIZE);
					tuple->t_data = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);
					res1 = recv(socks[i], tuple->t_data, tuple->t_len, 0);
					Assert(res1 == tuple->t_len);
					*res += res1;
					return tuple;
				}
				else if (*res < 0)
					perror("RECEIVE ERROR");
				else
				{
//					elog(LOG, "Close input socket: %d", socks[i]);
					closesocket(socks[i]);
					socks[i] = PGINVALID_SOCKET;
					continue;
				}
			}
		}
		*res = 0;
		return NULL;
}

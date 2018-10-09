#!/bin/bash

echo "Stopping gently and start postgres instances..."

. ./paths.sh

for (( node=0; node<$1; node++ ))
do
	echo "node: $node"
	pgdata_dir="PGDATA"$node
	port=$((5433+$node))
	echo "pgdata_dir: $pgdata_dir, port: $port"
	pg_ctl -D $pgdata_dir -o "-p $port" stop -m smart
	pg_ctl -c -D $pgdata_dir -l logfile$node -o "-p $port" start
done

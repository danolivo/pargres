#!/bin/bash

echo "Stopping all postgres instances..."

. ./paths.sh

for (( node=0; node<$1; node++ ))
do
	echo "node: $node"
	pgdata_dir="PGDATA"$node
	port=$((5433+$node))
	echo "pgdata_dir: $pgdata_dir, port: $port"
	pg_ctl -D $pgdata_dir -o "-p $port" stop
	rm -rf $pgdata_dir
	rm logfile$node
done

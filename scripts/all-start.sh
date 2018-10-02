#!/bin/bash

echo "Starting all postgres instances..."

. ./paths.sh
U=`whoami`

for (( node=0; node<$1; node++ ))
do
	echo "node: $node"
	pgdata_dir="PGDATA"$node
	port=$((5433+$node))
	echo "pgdata_dir: $pgdata_dir, port: $port"
	rm -rf $pgdata_dir
	mkdir $pgdata_dir
	initdb -D $pgdata_dir > 1
	echo "shared_preload_libraries = 'pargres'" >> $pgdata_dir/postgresql.conf
	echo "pargres.node = $node" >> $pgdata_dir/postgresql.conf
	echo "pargres.nnodes = $1" >> $pgdata_dir/postgresql.conf
	rm logfile$node
	pg_ctl -c -D $pgdata_dir -l logfile$node -o "-p $port" start
	createdb -p $port $U
done

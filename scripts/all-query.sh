#!/bin/bash

echo "Starting all postgres instances..."

. ./paths.sh

for (( node=0; node<$1; node++ ))
do
	port=$((5433+$node))
	echo "Send Query to node: $node, port: $port"
	psql -p $port -c "$2"
done

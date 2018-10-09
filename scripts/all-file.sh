#!/bin/bash

echo "Send File Query to all postgres instances..."

. ./paths.sh

for (( node=0; node<$1; node++ ))
do
	port=$((5433+$node))
	echo "Send File '$2' to node: $node, port: $port"
	psql -p $port -f $2
done

#!/bin/bash

echo "Send Query to all postgres instances..."

. ./paths.sh

for (( node=0; node<$1; node++ ))
do
	port=$((5433+$node))
	echo "Send command '$2' to node: $node, port: $port"
	$2 -p $port
done

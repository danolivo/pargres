#!/bin/bash

. ./paths.sh

port=$((5433+$1))
echo "COMMAND: Send '$2' to node: $node, port: $port"
$2 -p $port


#!/bin/bash

port=$((5433+$1))
echo "Send File '$2' to node: $1, port: $port"
psql -p $port -f $2


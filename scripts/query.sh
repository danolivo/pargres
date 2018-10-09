#!/bin/bash

echo "Send Query to one postgres instance..."

. ./paths.sh

port=$((5433+$1))
echo "Send Query '$2' to node: $1, port: $port"
psql -p $port -c "$2"


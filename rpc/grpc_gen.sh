#!/bin/sh
set -e -x
dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
echo "Changing to directory $dir"
cd $dir
protoc --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative ferry.proto

#!/bin/bash

cwd=$(dirname "$0")
project_root=$(dirname "$cwd")

#docker compose -f $cwd/compose.yml up -d
#trap "docker compose -f $cwd/compose.yml down" EXIT

#echo "Benchmarking Elastic"
#ELASTIC_AUTH="" ELASTIC_URL=http://localhost:9200 ELASTIC_INDEX=index_name WPS=1 BATCH_SIZE=50 DESTINATION=Elastic TRACK_LATENCY=true ./rockbench

Echo "Benchmarking CrateDB"
CRATEDB_URI="postgres://crate:@localhost:5432/test" WPS=1 BATCH_SIZE=50 DESTINATION=CrateDB TRACK_LATENCY=true ./rockbench
#!/bin/bash

set -x

# chdir to ext/
pushd "`dirname $0`/../"

# stop services if they are running
./bin/stop-test-cluster.sh

rm -rf var/cassandra/{commitlog,data,saved_caches}/*
rm -rf var/zookeeper/*

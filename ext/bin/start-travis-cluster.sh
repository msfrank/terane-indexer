#!/bin/bash

pushd "`dirname $0`" &>/dev/null
unset JVM_OPTS
source ./start-test-cluster.sh

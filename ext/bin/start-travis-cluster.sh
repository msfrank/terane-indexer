#!/bin/bash

pushd "`dirname $0`" &>/dev/null
unset JVM_OPTS
exec ./start-test-cluster.sh $@

#!/bin/bash

set -x

# chdir to ext/
pushd "`dirname $0`/../"

# check whether zookeeper is already running
RUNNING=0
PID=0
if [ -f var/run/zookeeper.pid ]; then
  PID=`cat var/run/zookeeper.pid`
  kill -0 `cat var/run/zookeeper.pid` &>/dev/null
  if [ "$?" -eq 0 ]; then
    RUNNING=1
  fi
fi

# start zookeeper if it is not running
if [ "$RUNNING" -eq 1 ]; then
  kill -TERM $PID &>/dev/null
  sleep 5
  kill -0 $PID &>/dev/null
  if [ "$?" -eq 0 ]; then
    echo "failed to stop zookeeper"
    exit 1
  else
    rm -f var/run/zookeeper.pid
  fi
fi

# check whether cassandra is already running
RUNNING=0
PID=0
if [ -f var/run/cassandra.pid ]; then
  PID=`cat var/run/cassandra.pid`
  kill -0 `cat var/run/cassandra.pid` &>/dev/null
  if [ "$?" -eq 0 ]; then
    RUNNING=1
  fi
fi

# start cassandra if it is not running
if [ "$RUNNING" -eq 1 ]; then
  kill -TERM $PID &>/dev/null
  sleep 5
  kill -0 $PID &>/dev/null
  if [ "$?" -eq 0 ]; then
    echo "failed to stop cassandra"
    exit 1
  else
    rm -f var/run/cassandra.pid
  fi
fi

exit 0

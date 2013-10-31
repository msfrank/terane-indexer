#!/bin/bash

set -x

# chdir do ext/
pushd "`dirname $0`/../"

# check whether zookeeper is already running
RUNNING=0
if [ -f var/zookeeper/zookeeper_server.pid ]; then
  kill -0 `cat var/zookeeper/zookeeper_server.pid` &>/dev/null
  if [ "$?" -eq 0 ]; then
    RUNNING=1
  else
    rm -f var/zookeeper/zookeeper_server.pid
  fi
fi

# start zookeeper if it is not running
if [ "$RUNNING" -eq 0 ]; then
  pushd lib/zookeeper
  ./bin/zkServer.sh start-foreground &
  PID=$$
  sleep 5
  kill -0 $PID &>/dev/null
  if [ "$?" -ne 0 ]; then
    echo "failed to start zookeeper"
    exit 1
  fi
  popd
fi

# check whether cassandra is already running
RUNNING=0
if [ -f var/cassandra/cassandra.pid ]; then
  kill -0 `cat var/cassandra/cassandra.pid` &>/dev/null
  if [ "$?" -eq 0 ]; then
    RUNNING=1
  else
    rm -f var/cassandra/cassandra.pid
  fi
fi

# start cassandra if it is not running
if [ "$RUNNING" -eq 0 ]; then
  pushd lib/cassandra
  ./bin/cassandra -f -p ../../var/cassandra/cassandra.pid &
  PID=$$
  sleep 5
  kill -0 $PID &>/dev/null
  if [ "$?" -ne 0 ]; then
    echo "failed to start cassandra"
    exit 1
  fi
  popd
fi

exit 0

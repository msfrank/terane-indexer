#!/bin/bash

if [ "$1" == "-v" ]; then
  set -x
fi

# chdir to ext/
pushd "`dirname $0`/../" &>/dev/null

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

# stop zookeeper if it is running
if [ "$RUNNING" -eq 1 ]; then
  echo "stopping zookeeper..."
  kill -TERM $PID &>/dev/null
  sleep 5
  kill -0 $PID &>/dev/null
  if [ "$?" -eq 0 ]; then
    echo "failed to stop zookeeper"
    exit 1
  else
    rm -f var/run/zookeeper.pid
    echo "stopped zookeeper (pid $PID)"
  fi
else
  echo "zookeeper is not running"
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

# stop cassandra if it is running
if [ "$RUNNING" -eq 1 ]; then
  echo "stopping cassandra..."
  kill -TERM $PID &>/dev/null
  sleep 5
  kill -0 $PID &>/dev/null
  if [ "$?" -eq 0 ]; then
    echo "failed to stop cassandra"
    exit 1
  else
    rm -f var/run/cassandra.pid
    echo "stopped cassandra (pid $PID)"
  fi
else
  echo "cassandra is not running"
fi

exit 0

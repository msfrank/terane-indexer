#!/bin/bash

if [ "$1" == "-v" ]; then
  set -x
fi

# chdir to ext/
pushd "`dirname $0`/../" &>/dev/null

# check whether zookeeper is already running
RUNNING=0
if [ -f var/run/zookeeper.pid ]; then
  PID=`cat var/run/zookeeper.pid`
  kill -0 $PID &>/dev/null
  if [ "$?" -eq 0 ]; then
    RUNNING=1
  else
    rm -f var/run/zookeeper.pid
  fi
fi

# start zookeeper if it is not running
if [ "$RUNNING" -eq 0 ]; then
  pushd lib/zookeeper &>/dev/null
  echo "starting zookeeper..."
  ./bin/zkServer.sh start-foreground 2>&1 1>/dev/null & 
  PID=$!
  sleep 5
  kill -0 $PID &>/dev/null
  if [ "$?" -ne 0 ]; then
    echo "failed to start zookeeper"
    exit 1
  else
    echo $PID > ../../var/run/zookeeper.pid
    echo "zookeeper is running (pid $PID)"
  fi
  popd &>/dev/null
else
  echo "zookeeper is running (pid $PID)"
fi

# check whether cassandra is already running
RUNNING=0
if [ -f var/run/cassandra.pid ]; then
  PID=`cat var/run/cassandra.pid`
  kill -0 $PID &>/dev/null
  if [ "$?" -eq 0 ]; then
    RUNNING=1
  else
    rm -f var/run/cassandra.pid
  fi
fi

# start cassandra if it is not running
if [ "$RUNNING" -eq 0 ]; then
  pushd lib/cassandra &>/dev/null
  echo "starting cassandra..."
  ./bin/cassandra -f 2>&1 1>/dev/null &
  PID=$!
  sleep 5
  kill -0 $PID &>/dev/null
  if [ "$?" -ne 0 ]; then
    echo "failed to start cassandra"
    exit 1
  else
    echo $PID > ../../var/run/cassandra.pid
    echo "cassandra is running (pid $PID)"
  fi
  popd &>/dev/null
else
  echo "cassandra is running (pid $PID)"
fi

exit 0

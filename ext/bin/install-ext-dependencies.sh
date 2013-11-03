#!/bin/bash

CASSANDRA="http://apache.osuosl.org/cassandra/1.2.11/apache-cassandra-1.2.11-bin.tar.gz"
ZOOKEEPER="http://apache.osuosl.org/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz"

set -x
set -e

# chdir to ext/
pushd "`dirname $0`/../"

# install zookeeper if necessary
if [ ! -f lib/zookeeper/bin/zkServer.sh ]; then
  rm -rf lib/zookeeper
  mkdir -p lib/zookeeper
  SRCPATH="$PWD/lib/src/`basename $ZOOKEEPER`"
  if [ ! -f $SRCPATH ]; then
    curl "$ZOOKEEPER" -o $SRCPATH
  fi
  pushd lib/zookeeper
  tar -xzv --strip-components 1 -f $SRCPATH
  pushd conf
  ln -f -s ../../../etc/zoo.cfg zoo.cfg
  ln -f -s ../../../etc/log4j.zookeeper.properties log4j.properties
  popd
  popd
fi

# install cassandra if necessary
if [ ! -f lib/cassandra/bin/cassandra ]; then
  rm -rf lib/cassandra
  mkdir -p lib/cassandra
  SRCPATH="$PWD/lib/src/`basename $CASSANDRA`"
  if [ ! -f $SRCPATH ]; then
    curl "$CASSANDRA" -o $SRCPATH
  fi
  pushd lib/cassandra
  tar -xzv --strip-components 1 -f $SRCPATH
  pushd conf
  ln -f -s ../../../etc/cassandra.yaml cassandra.yaml
  ln -f -s ../../../etc/log4j.cassandra.properties log4j-server.properties
  popd
  popd
fi

exit 0

#!/bin/bash

cd `dirname $0`
supervisord -c ../conf/terane_test_cluster_supervisord.conf -n

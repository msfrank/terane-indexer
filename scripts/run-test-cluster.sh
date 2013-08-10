#!/bin/bash

cd `dirname $0`
supervisord -c ../etc/terane_test_cluster_supervisord.conf -n

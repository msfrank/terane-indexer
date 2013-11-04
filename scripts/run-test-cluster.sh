#!/bin/bash

cd `dirname $0`
supervisord -c ../conf/supervisord.conf -n

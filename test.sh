#!/bin/bash

PYTHON="/usr/bin/env python26"
HADOOP_CONF=/etc/hadoop-0.20/conf
HADOOP_HOME=/usr/lib/hadoop-0.20

export CLASSPATH=${HADOOP_CONF}:$(find ${HADOOP_HOME} -name *.jar | sort | tr '\n' ':')

export PYTHONPATH=hdfs:${PYTHONPATH}

$PYTHON hdfs/hfilesystem_test.py
$PYTHON hdfs/hfile_test.py


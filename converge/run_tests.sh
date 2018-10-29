#!/bin/bash

if [ $1 == "local[*]" ]
then  
  spark-submit \
    --jars target/shred-test-converge-1.0.jar,../dep/gdb-spark-api-1.0.jar,target/lib/postgresql-9.4.1212.jre6.jar,target/lib/htsjdk-2.9.1.jar \
    --master local[*] \
    --driver-class-path target/lib/postgresql-9.4.1212.jre6.jar \
    --executor-memory 24G \
    --driver-memory 32G \
    --class cs.ox.ac.uk.shred.test.converge.App target/shred-test-converge-1.0.jar $@
else
  spark-submit \
    --jars target/shred-test-converge-1.0.jar,../dep/gdb-spark-api-1.0.jar,target/lib/postgresql-9.4.1212.jre6.jar,target/lib/htsjdk-2.9.1.jar \
    --master spark://192.168.11.235:7077 \
    --driver-class-path target/lib/postgresql-9.4.1212.jre6.jar \
    --executor-cores 4 \
    --num-executors 8 \
    --executor-memory 24G \
    --driver-memory 32G \
    --class cs.ox.ac.uk.shred.test.converge.App target/shred-test-converge-1.0.jar $@
fi


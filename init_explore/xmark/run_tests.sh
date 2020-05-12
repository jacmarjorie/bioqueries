#!/bin/bash

if [ $1 == "local[*]" ]
then  
  spark-submit \
    --jars target/shred-test-xmark-1.0.jar \
    --master $1 \
    --executor-memory 24G \
    --driver-memory 32G \
    --class cs.ox.ac.uk.shred.test.xmark.App target/shred-test-xmark-1.0.jar $@
else
  spark-submit \
    --jars target/shred-test-xmark-1.0.jar \
    --master $1 \
    --executor-cores 4 \
    --num-executors 9 \
    --executor-memory 28G \
    --driver-memory 32G \
    --class cs.ox.ac.uk.shred.test.xmark.App target/shred-test-xmark-1.0.jar $@
fi


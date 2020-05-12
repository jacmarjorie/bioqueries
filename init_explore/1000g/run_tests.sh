cp target/shred-test-onekg-1.0.jar /usr/lib/spark/jars/
hdfs dfs -rm -r shredding_q*
hdfs dfs -rm -r partitions_q*
spark-submit --jars target/shred-test-onekg-1.0.jar,../dep/gdb-spark-api-1.0.jar,/usr/lib/spark/jars/postgresql-9.4.1212.jre6.jar,/usr/lib/spark/jars/htsjdk-2.9.1.jar, --master yarn-client --driver-class-path /usr/lib/spark/jars/postgresql-9.4.1212.jre6.jar --class cs.ox.ac.uk.shred.test.onekg.App target/shred-test-onekg-1.0.jar
hdfs dfs -cat shredding_q*/part-00000-*.csv
#hdfs dfs -cat partitions_q*/part-00000-*.csv

### Running Example

1. Download the following jars: 
* [hadoop-bam-7.8.0.jar](https://repo1.maven.org/maven2/org/seqdoop/hadoop-bam/7.8.0/hadoop-bam-7.8.0.jar)
* [htsjdk-2.9.1.jar](https://repo1.maven.org/maven2/com/github/samtools/htsjdk/2.9.1/htsjdk-2.9.1.jar)

2. Start a small spark cluster:

```
export SPARK_HOME=/path/to/your/spark/folder
sudo sh $SPARK_HOME/start-master.sh
sudo sh $SPARK_HOME/start-slave.sh spark://<master-ip>:7077 -c 2 -m 4g
```

3. In your application file (probably App.scala) make sure you are pointing to the spark cluster in your Spark config:

```
val conf = new SparkConf().setMaster("spark://<master-ip>:7077")
```

4. Repartition your VCF Dataset, with a recommended 4 partitions per core: 

```
val variants = vloader.loadVCF.repartition(8)
``` 

5. Package application: `sbt package`

6. Run the spark application and request resources from this spark cluster:

```
spark-submit --class gwas.App \
      --master "spark://<master-ip>:7077" \
      --executor-cores 1 \
      --num-executors 2 \
      --executor-memory 2G \
      --jars /path/to/downloaded/jars/hadoop-bam-7.8.0.jar,/path/to/downloaded/jars/htsjdk-2.9.1.jar \
      --driver-memory 2G target/scala-2.12/gwas_2.12-0.1.jar
```

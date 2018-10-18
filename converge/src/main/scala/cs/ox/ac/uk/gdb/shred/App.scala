package cs.ox.ac.uk.shred.test.converge

/**
  * Query runner for genomic shredding experiments
  * Each query is performed for an increasing number of top level records (variants)
  * variant data is cached before performing the query
  */

import com.oda.gdbspark._
import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import htsjdk.variant.variantcontext.VariantContext

object App{

  def main(args: Array[String]){

    try{
      Class.forName("org.postgresql.Driver")
    }catch{
      case e: Exception => println(e)
    }

    val conf = new SparkConf()
                .setMaster("spark://192.168.11.235:7077")
                .setAppName("GDBShred")

    val spark = SparkSession.builder()
                  .config(conf)
                  .getOrCreate()

    import spark.sqlContext.implicits._

    val hostfile = "/mnt/app_hdd/scratch/flint-spark/hostfile"
    val loader = "/mnt/app_hdd/scratch/flint-spark/loader_converge_impute_WTomit.json"
    val ws = "/mnt/app_hdd/scratch/flint-spark/converge_all"
    val array = "chr10"
    val dbprops = "/mnt/app_hdd/scratch/flint-spark/db-properties.flat"

    val gdbmap = new GDBMapper(spark, dbprops)
    val gdb = new GDBConnector(spark.sparkContext, loader, hostfile, gdbmap, ws, array)

    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:postgresql://192.168.11.249/jflint")
                .option("dbtable", "converge").option("user", "jflint").option("password", "jflint").load()

    val samples = gdbmap.query("callset").rdd.collect.map(r => r(1).toString).toList

    val clinic = jdbcDF.where("iscase is not null")
    
    val clincBroadcast = spark.sparkContext.broadcast(jdbcDF.where("iscase is not null")) 
    
    val query_regions = List(
      List(("10", 1, 200000)), //734
      List(("10", 1, 500000)), //3667
      List(("10", 1, 800000)), //7387
      List(("10", 1, 1000000)), //9031
      List(("10", 1, 1200000)), //11004
      List(("10", 1, 1500000)), //14143
      List(("10", 1, 2000000)), //19927
      List(("10", 1, 5000000)) //50895
    )
   
    if(args contains "1"){
      // group by binary variable
      val q1 = Query1
      for(region <- query_regions){
      
        val variants = gdb.queryByRegion(samples, region, false).map(x => x._2).repartition(4)
        variants.cache
        val c = variants.count
      
        for(i <- 1 to 1){
          q1.testFlat(c, variants, clinic)
        }
        for(i <- 1 to 1){
          q1.testShred(c, variants, clinic)
        }
      }
      q1.close()
    }

    if(args contains "1bc"){
      // group by binary variable, broadcast clinical data
      val q1bc = Query1Broadcast
      for(region <- query_regions){
      
        val variants = gdb.queryByRegion(samples, region, false).map(x => x._2).repartition(4)
        variants.cache
        val c = variants.count
  
        for(i <- 1 to 1){
          q1bc.testQ1(c, variants, clincBroadcast)
        }
        for(i <- 1 to 1){
          q1bc.testQ1_shred(c, variants, clincBroadcast)
        }
      }
      q1bc.close()
    }

    if(args contains "2"){
      // group by categorical variable
      val q2 = Query2
      for(region <- query_regions){
      
        val variants = gdb.queryByRegion(samples, region, false).map(x => x._2).repartition(4)

        variants.cache
        val c = variants.count
  
        for(i <- 1 to 1){
          q2.testQ2(c, variants, clinic)
        }
        for(i <- 1 to 1){
          q2.testQ2_shred(c, variants, clinic)
        }
      }
      q2.close()
    }

    if(args contains "3"){
      // query 3 uses xml from i2b2
      val q3 = Query3(spark, "/nfs/home/jaclyns/jflint/gdb-spark-api/crc_ILnHKHDULm.xml")
      for(region <- query_regions){
      
        val variants = gdb.queryByRegion(samples, region, false).map(x=>x._2).repartition(4)
        variants.cache
        val c = variants.count
  
        for(i <- 1 to 1){
          q3.testQ3(c, variants)
        }
        for(i <- 1 to 1){
          q3.testQ3_shred(c, variants)
        }
      }
      q3.close()
    }

    if(args contains "4"){
      val q4 = Query4
      for(region <- query_regions){
      
        var variants = gdb.queryByRegion(samples, region, false).map(x=>x._2).repartition(4)
        variants.cache
        val c = variants.count
      
        // map to dbsnp id
        //val s = "(SELECT * FROM snpchrposonref WHERE (chr, pos) IN ("+variants.map(r => 
         //   ("'"+r.getContig+"'", "'"+Integer.toString(r.getStart.asInstanceOf[Int]-1)+"'")).collect.toList.distinct.mkString(",")+")) AS snptable"
        //val snps = gdbmap.query(s).rdd.map(r => (r.getString(1), r.getInt(2)+1) -> r.getInt(0))
        val snps: RDD[((String, Int), Int)] = spark.sparkContext.emptyRDD

        for(i <- 1 to 1){
          q4.testFlat(c, variants, clinic, snps)
        }
        for(i <- 1 to 1){
          q4.testShred(c, variants, clinic, snps)
        }
      } 
      q4.close()
    }
  }
}

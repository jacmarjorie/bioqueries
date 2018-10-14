package cs.ox.ac.uk.shred.test.converge

import com.oda.gdbspark._
import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import htsjdk.variant.variantcontext.{Genotype, VariantContext}

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
                  .config("spark.dynamicAllocation.minExecutors", "2")
                  .config("spark.executor.memory", "12G")
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
      List("rs12415800", "rs35936514"),
      List("PTEN"), 
      List("PTEN", "SIRT1"),  
      List("PTEN", "SIRT1", "LHPP"),
      List("PTEN", "SIRT1", "LHPP", "KIN"),
      List("PTEN", "SIRT1", "LHPP", "KIN", "EGR2", "FRAT1", "WASHC2C")
    )
    
    // group by binary variable
    val q1 = Query1
    for(region <- query_regions){
      
      val variants = gdb.queryByGene(samples, region, false).map(x=>x._2)
      variants.cache
      val c = variants.count
  
      for(i <- 1 to 1){
        q1.testQ1(c, variants, clinic)
      }
      for(i <- 1 to 1){
        q1.testQ1_shred(c, variants, clinic)
      }
    }
    q1.close()

    // group by binary variable, broadcast clinical data
    val q1bc = Query1Broadcast
    for(region <- query_regions){
      
      val variants = gdb.queryByGene(samples, region, false).map(x=>x._2)
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

    // group by categorical variable
    val q2 = Query2
    for(region <- query_regions){
      
      val variants = gdb.queryByGene(samples, region, false).map(x=>x._2)
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

    // query 3 uses xml from i2b2
    val q3 = Query3(spark, "/nfs/home/jaclyns/jflint/gdb-spark-api/crc_ILnHKHDULm.xml")
    for(region <- query_regions){
      
      val variants = gdb.queryByGene(samples, region, false).map(x=>x._2)
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
}

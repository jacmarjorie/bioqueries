package cs.ox.ac.uk.shred.test.onekg

import com.oda.gdbspark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App{

  def main(args: Array[String]){

    try{
      Class.forName("org.postgresql.Driver")
    }catch{
      case e: Exception => println(e)
    }

    val conf = new SparkConf()
                .setMaster("yarn")
                .setAppName("GDBShred")

    val spark = SparkSession.builder()
                  .config(conf)
                  .getOrCreate()

    import spark.sqlContext.implicits._

    val hostfile = "/home/hadoop/hostfile"
    val loader = "/mnt/json/loader_config_file.json"
    val ws = "s3://oda-iph-iph-genomics-analysis-emrbucket-dev-bucket/1000g_ph3_2492"
    val array = "part0"
    val dbprops = "/home/hadoop/db-properties.flat"
    val gdbmap = new GDBMapper(spark, dbprops)
    val gdb = new GDBConnector(spark.sparkContext, loader, hostfile, gdbmap, ws, array)

    val samples = gdbmap.query("(SELECT name FROM CALLSET WHERE id < 2492) AS tmp").rdd.map(p => p(0).toString).collect().toList
    val clinic = gdbmap.query("test_1000g").where($"individual_id".isin(samples:_*))
    
    val clincBroadcast = spark.sparkContext.broadcast(clinic) 
    
    val query_regions = List(
      List("PTEN"),
      List("PTEN", "TP53"),
      List("PTEN", "TP53","ACADM"),
      List("PTEN", "TP53","ACADM","NGF"),
      List("PTEN", "TP53","ACADM","NGF","NOTCH2"),
      List("PTEN", "TP53","ACADM","NGF","NOTCH2","SIRT1"),
      List("PTEN", "TP53","ACADM","NGF","NOTCH2","SIRT1", "LHPP"),
      List("PTEN", "TP53","ACADM","NGF","NOTCH2","SIRT1", "LHPP", "BRCA1"),
      List("PTEN", "TP53","ACADM","NGF","NOTCH2","SIRT1", "LHPP", "BRCA1", "TGFBI", "GPBP1"),
      List("PTEN", "TP53","ACADM","NGF","NOTCH2","SIRT1", "LHPP", "BRCA1", "TGFBI", "GPBP1", "ECOP", "ACTR3B")
    )
    
    // group by binary variable
    val q1 = Query1(spark)
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
    q1.writeResult()
    
    // group by binary variable, broadcast clinical data
    val q1bc = Query1Broadcast(spark)
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
    q1bc.writeResult()

    // group by categorical variable
    val q2 = Query2(spark)
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
    q2.writeResult()
  }
}

package cs.ox.ac.uk.shred.test.xmark

/**
  * Query runner for xmark shredding experiments
  * xmark benchmark: https://projects.cwi.nl/xmark/downloads.html
  */

import scala.xml._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import collection.JavaConversions._
import collection.JavaConverters._

object App{

  def main(args: Array[String]){

    val argsList = args.toList
    val queries = argsList.tail
    val tlrecords = 100
    val parts = 72
    val xpath = "/mnt/app_hdd/scratch/xmark/test"
    val conf = new SparkConf()
                .setMaster(argsList(0))
                .setAppName("XMarkShred")

    val spark = SparkSession.builder()
                  .config(conf)
                  .getOrCreate()

    val inc = List(1, 10, 25, 50, 100)
    val tests = inc.map{case i => xpath+i+".xml"}   

    if(queries contains "8"){

      val x8 = XMark8

      for(test <- tests){
        val label = test.split("/").last.replace(".xml", "")
        val xreader = new XReader(spark.sparkContext, test, parts)
        val auctions = xreader.auctions    
        auctions.cache
        auctions.count

        if(queries contains "flat"){
          println("executing "+label+" test")
          for(i <- 1 to 1){
            x8.flat(auctions, label)
          }
        }

        if(queries contains "shred"){
          for(i <- 1 to 1){
            x8.shred(xreader, auctions, label)
          }
        }
        
        if(queries contains "shredopt"){
          for(i <- 1 to 1){
            x8.shredOpt(xreader, auctions, label)
          }
        }

      }
    }

  }
}

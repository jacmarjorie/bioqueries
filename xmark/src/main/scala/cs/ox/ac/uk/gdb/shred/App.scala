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
    val parts = 36
    val xpath = "/mnt/app_hdd/scratch/xmark/test"
    val conf = new SparkConf()
                .setMaster(argsList(0))
                .setAppName("XMarkShred")

    val spark = SparkSession.builder()
                  .config(conf)
                  .getOrCreate()

    //val tests = List("1,1000", "1,2000", "1,5000", "1,10000", "1,20000", "1,40000")   
    val tests = List("10,1000", "10,2000", "10,5000", "10,10000", "10,20000", "10,40000")   

    if(queries contains "8"){

      val x8 = XMark8

      for(test <- tests){
        val xreader = new XReader(spark.sparkContext, parts)
        println("done x reading")
        val auctions = xreader.getSites(xpath+test.replace(",", "_"))
        //auctions.cache
        val c = auctions.count
        println(c)
        //println(auctions.mapPartitionsWithIndex{
        //    case (i,rows) => Iterator((i,rows.size))
        //}.collect.toSeq)

        if(queries contains "flat"){
          for(i <- 1 to 1){
            x8.flat(auctions, test+","+c)
          }
        }

        if(queries contains "shred"){
          for(i <- 1 to 1){
            x8.shred(xreader, auctions, test+","+c)
          }
        }
        
        if(queries contains "shredopt"){
          for(i <- 1 to 1){
            x8.shredOpt(xreader, auctions, test+","+c)
          }
        }

      }
    }

  }
}

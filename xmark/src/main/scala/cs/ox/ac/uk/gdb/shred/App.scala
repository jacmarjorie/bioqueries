package cs.ox.ac.uk.shred.test.xmark

/**
  * Query runner for xmark shredding experiments
  * xmark benchmark: https://projects.cwi.nl/xmark/downloads.html
  */

import scala.xml._

object App{

  def main(args: Array[String]){

    val argsList = args.toList
    val queries = argsList.tail
    val repartition = 72
    val xpath = "/nfs/home/jaclyns/xmark/test"
    val conf = new SparkConf()
                .setMaster(argsList(0))
                .setAppName("XMarkShred")

    val spark = SparkSession.builder()
                  .config(conf)
                  .getOrCreate()

    val tests = List.fill(argsList(0))(xpath).zipWithIndex.map{case (xp, i) => xp+i+".xml"}   

    if(queries contains "8"){

      val x8 = XMark8

      for(test <- tests){
        if(queries contains "flat"){
          for(i <- 1 to 1){
            x8.testFlat(c, variants, clinic)
          }
        }

        if(queries contains "shred"){
          for(i <- 1 to 1){
            q1.testShred(c, variants, clinic)
          }
        }
      }

      q1.close()
    }

  }
}

package cs.ox.ac.uk.shred.test.onekg

import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext._
import org.apache.spark.sql.SparkSession

object Utils extends Serializable{

    def reportGenotypeType(gt: Genotype): Int = {
      gt match {
        case gt if gt.isHomRef => 0;
        case gt if gt.isHet => 1;
        case gt if gt.isHomVar => 2;
        case gt if gt.isNoCall => 3;
        case _ => 4;
      }
    }

    def shred(rdd: RDD[VariantContext]) = {
      val lbl = rdd.zipWithUniqueId
      val flat = lbl.map( i => i match { case (x,l) => (x.getContig, x.getStart, l) })
      val dict = lbl.map( i => i match { case (x,l) => (l, x.getGenotypesOrderedByName) })
      (flat,dict)
    }

    def writeResult(spark_session: SparkSession, query: List[(String, String, Long, Long)], parts: List[(String, String, Long, Int, Int)], name: String, get_skew: Boolean): Unit = {
      import spark_session.implicits._
      val df = query.toDF("data", "query", "region", "time")
      df.coalesce(1).write
        .format("com.databricks.spark.csv")
        .mode("overwrite")
        .save("shredding_"+name)
      if(get_skew){
        val df2 = parts.toDF("data", "query", "region", "partition", "time")
        df2.coalesce(1).write
          .format("com.databricks.spark.csv")
          .mode("overwrite")
          .save("partitions_"+name)
      }
    }
}

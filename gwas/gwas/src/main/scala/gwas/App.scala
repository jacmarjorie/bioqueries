package gwas

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.oda.gdbspark._
import htsjdk.variant.variantcontext.VariantContext

object App {
  def main(args: Array[String]){
    try{
      Class.forName("org.postgresql.Driver")
    }catch{
      case e: Exception => println(e)
    }

    val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("GWAS")

    val spark = SparkSession.builder()
              .config(conf)
              .getOrCreate()

    val vloader = new VariantLoader(spark)
    val cloader = new ClinicalLoader(spark)

    val cinfo = cloader.clinical
    val samples = cinfo.select("id").rdd.map(r => r.getString(0)).take(10).toList

    val vinfo = vloader.getVariants(samples, List("SIRT1"))
    vinfo.take(10).foreach(println(_))

  }
}
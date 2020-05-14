package gwas

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.LongWritable
import htsjdk.variant.variantcontext.{CommonInfo, VariantContext}
import org.seqdoop.hadoop_bam.{VCFInputFormat, VariantContextWritable}

class VariantLoader(spark: SparkSession, path: String) extends Serializable {

  def loadVCF(): RDD[VariantContext] = {
     spark.sparkContext
      .newAPIHadoopFile[LongWritable, VariantContextWritable, VCFInputFormat](path)
      .map(_._2.get)
   }

}
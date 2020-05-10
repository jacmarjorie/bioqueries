package burden

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.LongWritable
import htsjdk.variant.variantcontext.{CommonInfo, VariantContext}
import org.seqdoop.hadoop_bam.{VCFInputFormat, VariantContextWritable}


object App {
  
  def main(args: Array[String]){

   // standard setup
   val conf = new SparkConf().setMaster("local[*]")
     .setAppName("GeneBurden")
   val spark = SparkSession.builder().config(conf).getOrCreate()


   def loadVCF(path: String): RDD[VariantContext] = {
     spark.sparkContext
      .newAPIHadoopFile[LongWritable, VariantContextWritable, VCFInputFormat](path)
      .map(_._2.get)
   }

   // update this with your info
   val basepath = "/Users/jac/Downloads/"
   val variants = loadVCF(basepath+"ALL.wgs.integrated_phase1_release_v3_coding_annotation.20101123.snps_indels.sites.vcf")
  
   // example of accessing attributes in the VariantContext object
   val example = variants.map(v => (v.getContig, v.getStart))
   variants.take(10).foreach(println(_))
   spark.stop()

  }
}


package gwas

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]){

    val conf = new SparkConf()
            .setMaster("spark://Jac:7077")
            .setAppName("GWAS")

    val spark = SparkSession.builder()
              .config(conf)
              .getOrCreate()

    val vloader = new VariantLoader(spark, "/Users/jac/bioqueries/data/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf")
    val variants = vloader.loadVCF()

    variants.take(1).foreach(println(_))

    val cloader = new ClinicalLoader(spark)
    cloader.tgenomes.take(10).foreach(println(_))

  }

}
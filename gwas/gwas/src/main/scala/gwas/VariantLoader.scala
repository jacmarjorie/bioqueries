package gwas

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.LongWritable
import htsjdk.variant.variantcontext.{CommonInfo, VariantContext, Genotype}
import org.seqdoop.hadoop_bam.{VCFInputFormat, VariantContextWritable}

case class Call(g_sample: String, call: Int)
case class Variant(contig: String, start: Int, reference: String, 
  alternate: String, genotypes: Seq[Call])
case class IVariant(index: Long, contig: String, start: Int, reference: String, 
  alternate: String, genotypes: Seq[Call])

case class SCall(_1: Long, g_sample: String, call: Int)
case class SVariant(contig: String, start: Int, reference: String, 
  alternate: String, genotypes: Long)

class VariantLoader(spark: SparkSession, path: String) extends Serializable {

  import spark.implicits._

  implicit val genotypeEncoder = Encoders.product[Call]
  implicit val variantEncoder = Encoders.product[Variant]

  def loadVCF: RDD[VariantContext] = {
     spark.sparkContext
      .newAPIHadoopFile[LongWritable, VariantContextWritable, VCFInputFormat](path)
      .map(_._2.get)
   }

  def loadDS: Dataset[Variant] = {
     spark.sparkContext
      .newAPIHadoopFile[LongWritable, VariantContextWritable, VCFInputFormat](path)
      .map{ case (k, v) =>
        val variant = v.get
        val genotypes = variant.getGenotypes.iterator.asScala.toSeq.map(s => Call(s.getSampleName, callCategory(s)))
        Variant(variant.getContig, variant.getStart, variant.getReference.toString, 
          variant.getAlternateAllele(0).toString, genotypes)
      }.toDF().as[Variant]    
  }

  def shredDS: (Dataset[SVariant], Dataset[SCall]) = {
    val input = loadDS.withColumn("index", monotonically_increasing_id()).as[IVariant]
    val variants = input.drop("genotypes").withColumnRenamed("index", "genotypes").as[SVariant]
    val genotypes = input.flatMap{
      case variant => variant.genotypes.map{
        case genotype => SCall(variant.index, genotype.g_sample, genotype.call)
      }
    }.as[SCall]
    (variants, genotypes.repartition($"_1"))
  }

  private def callCategory(g: Genotype): Int = g match {
    case c if g.isHet => 1
    case c if g.isHomVar => 2
    case _ => 0
   }

}
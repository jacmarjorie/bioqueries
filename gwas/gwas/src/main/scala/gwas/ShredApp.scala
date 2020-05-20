package gwas

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang._

case class Recordfe8fcfcc57534c38b6334da6f50be0ab(reference: String, calls: Long, alternate: String, contig: String, start: Int)
case class Recordc6bf406287fe4c66a6877e04a938d35e(g_sample: String, population: String, family_id: String, _1: Long, m_sample: String, call: Int, gender: String)
case class Record55942073fb6a41298291d689554f33e8(gender: String, refCnt: Double, altCnt: Double, _1: Long)
case class Record19c362ae39e046a78bf374222dc33f0b(gender: String, _1: Long, refCnt: Double, altCnt: Double)
case class Record954d8b4b27ee4e8d891ae4b9edca39c0(_1: Long, gender: String)
case class Record5b43a93278e44a1eb76d143966dac829(reference: String, calls: Long, altCnt: Double, refCnt: Double, alternate: String, contig: String, _1: Long, start: Int, gender: String)
case class Recordff78d656a404406da5602367d9c15176(reference: String, maleRatio: Double, alternate: String, femaleRatio: Double, contig: String, start: Int)
case class Recordb921716887774665b7c16c66bc75db7d(contig: String, start: Int, reference: String, alternate: String)
case class Record4656753e41ef424a878c6f4e36f5031f(reference: String, odds: Double, alternate: String, contig: String, start: Int)

object ShredApp extends App{
  override def main(args: Array[String]){

    val conf = new SparkConf()
            .setMaster("spark://Jac:7077")
            .setAppName("GWAS")

    val spark = SparkSession.builder()
              .config(conf)
              .getOrCreate()

    import spark.implicits._

    val vloader = new VariantLoader(spark, "/Users/jac/bioqueries/data/sub.vcf")
    val (variants, genotypes) = vloader.shredDS
    val IBag_variants__D = variants
    val IDict_variants__D_genotypes = genotypes
    val cloader = new ClinicalLoader(spark)
    val IBag_metadata__D = cloader.tgenomes

val x32 = IBag_variants__D 
val x34 = x32
          .withColumnRenamed("genotypes", "calls").as[Recordfe8fcfcc57534c38b6334da6f50be0ab]
 
val x35 = x34
val MBag_ORStep1_1 = x35
//MBag_ORStep1_1.print
//MBag_ORStep1_1.cache
MBag_ORStep1_1.count
val x37 = IDict_variants__D_genotypes 
val x40 = x37.join(IBag_metadata__D, 
 col("g_sample") === col("m_sample")).as[Recordc6bf406287fe4c66a6877e04a938d35e]
 
val x42 = x40.withColumn("refCnt", when(col("call") === 1, 1.0).otherwise(when(col("call") === 2, 0.0).otherwise(2.0)))
.withColumn("altCnt", when(col("call") === 1, 1.0).otherwise(when(col("call") === 2, 2.0).otherwise(0.0)))
          .as[Record55942073fb6a41298291d689554f33e8]
 
val x44 = x42.groupByKey(x43 => Record954d8b4b27ee4e8d891ae4b9edca39c0(x43._1, x43.gender))
 .agg(typed.sum[Record55942073fb6a41298291d689554f33e8](x43 => x43.refCnt)
,typed.sum[Record55942073fb6a41298291d689554f33e8](x43 => x43.altCnt)
).mapPartitions{ it => it.map{ case (key, refCnt, altCnt) =>
   Record19c362ae39e046a78bf374222dc33f0b(key.gender, key._1, refCnt, altCnt)
}}.as[Record19c362ae39e046a78bf374222dc33f0b]
 
val x45 = x44
val MDict_ORStep1_1_calls_1 = x45.repartition($"_1")
//MDict_ORStep1_1_calls_1.print
//MDict_ORStep1_1_calls_1.cache
MDict_ORStep1_1_calls_1.count
val x47 = MBag_ORStep1_1 
val x50 = x47.join(MDict_ORStep1_1_calls_1, 
 col("calls") === col("_1"), "left_outer").as[Record5b43a93278e44a1eb76d143966dac829]
 
val x52 = x50.withColumn("maleRatio", when(col("gender") === "male", when(col("refCnt") === 0.0, 0.0).otherwise(col("altCnt") / col("refCnt"))).otherwise(0.0))
.withColumn("femaleRatio", when(col("gender") === "female", when(col("refCnt") === 0.0, 0.0).otherwise(col("altCnt") / col("refCnt"))).otherwise(0.0))
          .as[Recordff78d656a404406da5602367d9c15176]
 
val x54 = x52.groupByKey(x53 => Recordb921716887774665b7c16c66bc75db7d(x53.contig, x53.start, x53.reference, x53.alternate))
 .agg(typed.sum[Recordff78d656a404406da5602367d9c15176](x53 => x53.maleRatio)
,typed.sum[Recordff78d656a404406da5602367d9c15176](x53 => x53.femaleRatio)
).mapPartitions{ it => it.map{ case (key, maleRatio, femaleRatio) =>
   Recordff78d656a404406da5602367d9c15176(key.reference, maleRatio, key.alternate, femaleRatio, key.contig, key.start)
}}.as[Recordff78d656a404406da5602367d9c15176]
 
val x55 = x54
val MBag_ORStep2_1 = x55
//MBag_ORStep2_1.print
//MBag_ORStep2_1.cache
MBag_ORStep2_1.count
val x57 = MBag_ORStep2_1 
val x59 = x57.withColumn("odds", when(col("maleRatio") === 0.0, 0.0).otherwise(col("femaleRatio") / col("maleRatio")))
          .as[Record4656753e41ef424a878c6f4e36f5031f]
 
val x60 = x59
val MBag_OddsRatio_1 = x60
//MBag_OddsRatio_1.print
//MBag_OddsRatio_1.cache
MBag_OddsRatio_1.collect.foreach(println(_))
  }

}
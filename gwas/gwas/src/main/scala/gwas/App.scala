package gwas

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang._

case class Record6b117d219d084a15ad68f36f73a5495c(g_sample: String, call: Int)
case class Record3e020d5d120f42afba76f3c36f33e76b(reference: String, genotypes: Seq[Record6b117d219d084a15ad68f36f73a5495c], alternate: String, variants_index: Long, contig: String, start: Int)
case class Record724565d38152420489ffd38c187666b8(reference: String, genotypes: Seq[Record6b117d219d084a15ad68f36f73a5495c], alternate: String, variants_index: Long, contig: String, start: Int, genotypes_index: Long)
case class Recordc4140665d5e54f1293d316873bec92bf(reference: String, g_sample: Option[String], alternate: String, variants_index: Long, contig: String, start: Int, call: Option[Int], genotypes_index: Long)
case class Recorde47f1861ff3d40469ad3376ea93fe11b(population: String, family_id: String, m_sample: String, metadata_index: Long, gender: String)
case class Record30e8c577ae9f4e6d9a697eeb2b5a182f(reference: String, g_sample: Option[String], population: Option[String], family_id: Option[String], alternate: String, variants_index: Long, contig: String, m_sample: Option[String], metadata_index: Option[Long], start: Int, call: Option[Int], genotypes_index: Long, gender: Option[String])
case class Record14ed2d17c2b442fa972f5c2fab3a5ebe(reference: String, altCnt: Double, refCnt: Double, alternate: String, variants_index: Long, contig: String, start: Int, gender: Option[String])
case class Recordafe29ffb38944c45a52ec7ec715907a7(reference: String, alternate: String, variants_index: Long, contig: String, start: Int, gender: Option[String])
case class Record0144fddc9d2848c08f90477906a9a687(reference: String, alternate: String, variants_index: Long, contig: String, start: Int)
case class Record02d2a101777b4d068d7d5fde3d3d5ca5(gender: String, refCnt: Double, altCnt: Double)
case class Record46f24ca4404c410bac9901bea863a3a7(reference: String, _2: Seq[Record02d2a101777b4d068d7d5fde3d3d5ca5], alternate: String, variants_index: Long, contig: String, start: Int)
case class Recordef6ad2d3c2d74f838c1a57de31e8cf2e(reference: String, calls: Seq[Record02d2a101777b4d068d7d5fde3d3d5ca5], alternate: String, contig: String, start: Int)
case class Recordd1c135db705944e8a1a3165a2727da40(reference: String, calls: Seq[Record02d2a101777b4d068d7d5fde3d3d5ca5], alternate: String, ORStep1_index: Long, contig: String, start: Int)
case class Recordf3dcde1a3ed243779e80476f20c16289(reference: String, altCnt: Double, refCnt: Double, alternate: String, ORStep1_index: Long, contig: String, start: Int, gender: String)
case class Recordb54acab0b9d949d8a65e428c7a4c95c3(reference: String, maleRatio: Double, alternate: String, femaleRatio: Double, contig: String, start: Int)
case class Record8a97562c78a64878b42c891fc590eae3(contig: String, start: Int, reference: String, alternate: String)
case class Record0878c9d94fb24feca494630c8fcc53fe(reference: String, maleRatio: Double, alternate: String, femaleRatio: Double, contig: String, start: Int, ORStep2_index: Long)
case class Record8925464e5ac642f2a363391f040e6c69(reference: String, odds: Double, alternate: String, contig: String, start: Int)

object App {
  def main(args: Array[String]){

    val conf = new SparkConf()
            .setMaster("spark://Jac:7077")
            .setAppName("GWAS")

    val spark = SparkSession.builder()
              .config(conf)
              .getOrCreate()

    import spark.implicits._

    val vloader = new VariantLoader(spark, "/Users/jac/bioqueries/data/sub.vcf")
    val variants = vloader.loadDS
    val cloader = new ClinicalLoader(spark)
    val metadata = cloader.tgenomes

 val x36 = variants.withColumn("variants_index", monotonically_increasing_id())
 .as[Record3e020d5d120f42afba76f3c36f33e76b]
 
val x38 = x36 
val x39 = x38.withColumn("genotypes_index", monotonically_increasing_id())
 .as[Record724565d38152420489ffd38c187666b8]
 
// flatten the genotype information from the vcf
val x42 = x39.flatMap{
 case x40 => if (x40.genotypes.isEmpty) Seq(Recordc4140665d5e54f1293d316873bec92bf(x40.reference, None, x40.alternate, x40.variants_index, x40.contig, x40.start, None, x40.genotypes_index))
   else x40.genotypes.map( x41 => Recordc4140665d5e54f1293d316873bec92bf(x40.reference, Some(x41.g_sample), x40.alternate, x40.variants_index, x40.contig, x40.start, Some(x41.call), x40.genotypes_index) )
}.as[Recordc4140665d5e54f1293d316873bec92bf]

// this is the metadata file with the population information
val x43 = metadata.withColumn("metadata_index", monotonically_increasing_id())
 .as[Recorde47f1861ff3d40469ad3376ea93fe11b]
 
// join the sample information (extracted from the flattening above) with the population information
val x46 = x42.join(x43, 
 col("g_sample") === col("m_sample"), "left_outer").as[Record30e8c577ae9f4e6d9a697eeb2b5a182f]
 
// this counts the number of alternate and reference alleles
// when call == 1 this is heterozygous, meanining one copy of alt and one copy of ref 
// when call == 2 this is homozygous alternate, meaning two copies of the alternate allele 
// when call == 0 this is homozygous reference (wild type), meaning two copies of the reference allele
val x48 = x46.withColumn("altCnt", when(col("call") === 1, 1.0).otherwise(when(col("call") === 2, 2.0).otherwise(0.0)))
.withColumn("refCnt", when(col("call") === 1, 1.0).otherwise(when(col("call") === 2, 0.0).otherwise(2.0))).as[Record14ed2d17c2b442fa972f5c2fab3a5ebe]

// once I count the number of reference and alternate alleles, I used dataset.agg() to sum the reference and alternate alleles for each variant
// i'm using gender information from the population file to create a male and female cohort for each variant
// this is how you sumBy
val x50 = x48.groupByKey(x49 => Recordafe29ffb38944c45a52ec7ec715907a7(x49.reference, x49.alternate, x49.variants_index, x49.contig, x49.start, x49.gender))
 .agg(typed.sum[Record14ed2d17c2b442fa972f5c2fab3a5ebe](x49 => x49.refCnt)
,typed.sum[Record14ed2d17c2b442fa972f5c2fab3a5ebe](x49 => x49.altCnt)
).mapPartitions{ it => it.map{ case (key, refCnt, altCnt) =>
   Record14ed2d17c2b442fa972f5c2fab3a5ebe(key.reference, altCnt, refCnt, key.alternate, key.variants_index, key.contig, key.start, key.gender)
}}.as[Record14ed2d17c2b442fa972f5c2fab3a5ebe]
 
// now that i have the counts, I group by variants and gender to get the nested data structure back:
// {(contig, start, reference, alternate, cohorts := {(gender, refCnt, altCnt)} )}
// this how you group back up
val x52 = x50.groupByKey(x51 => Record0144fddc9d2848c08f90477906a9a687(x51.reference, x51.alternate, x51.variants_index, x51.contig, x51.start)).mapGroups{
 case (key, value) => 
   val grp = value.flatMap(x51 => 
    x51.gender match {
      case None => Seq()
      case _ => Seq(Record02d2a101777b4d068d7d5fde3d3d5ca5(x51.gender.get, x51.refCnt, x51.altCnt))
   }).toSeq
   Record46f24ca4404c410bac9901bea863a3a7(key.reference, grp, key.alternate, key.variants_index, key.contig, key.start)
 }.as[Record46f24ca4404c410bac9901bea863a3a7]
 
val x54 = x52.withColumnRenamed("_2", "calls").as[Recordef6ad2d3c2d74f838c1a57de31e8cf2e]
 
val x55 = x54
val ORStep1 = x55
//ORStep1.print
//ORStep1.cache
//ORStep1.count
val x56 = ORStep1.withColumn("ORStep1_index", monotonically_increasing_id())
 .as[Recordd1c135db705944e8a1a3165a2727da40]
 
val x58 = x56 
val x61 = x58.flatMap{ case x59 => 
 x59.calls.map( x60 => Recordf3dcde1a3ed243779e80476f20c16289(x59.reference, x60.altCnt, x60.refCnt, x59.alternate, x59.ORStep1_index, x59.contig, x59.start, x60.gender) )
}.as[Recordf3dcde1a3ed243779e80476f20c16289]
 
val x63 = x61.withColumn("maleRatio", when(col("gender") === "male", when(col("refCnt") === 0.0, 0.0).otherwise(col("altCnt") / col("refCnt"))).otherwise(0.0))
.withColumn("femaleRatio", when(col("gender") === "female", when(col("refCnt") === 0.0, 0.0).otherwise(col("altCnt") / col("refCnt"))).otherwise(0.0)).as[Recordb54acab0b9d949d8a65e428c7a4c95c3]
 
val x65 = x63.groupByKey(x64 => Record8a97562c78a64878b42c891fc590eae3(x64.contig, x64.start, x64.reference, x64.alternate))
 .agg(typed.sum[Recordb54acab0b9d949d8a65e428c7a4c95c3](x64 => x64.maleRatio)
,typed.sum[Recordb54acab0b9d949d8a65e428c7a4c95c3](x64 => x64.femaleRatio)
).mapPartitions{ it => it.map{ case (key, maleRatio, femaleRatio) =>
   Recordb54acab0b9d949d8a65e428c7a4c95c3(key.reference, maleRatio, key.alternate, femaleRatio, key.contig, key.start)
}}.as[Recordb54acab0b9d949d8a65e428c7a4c95c3]
 
val x66 = x65
val ORStep2 = x66
//ORStep2.print
//ORStep2.cache
//ORStep2.count
val x67 = ORStep2.withColumn("ORStep2_index", monotonically_increasing_id())
 .as[Record0878c9d94fb24feca494630c8fcc53fe]
 
val x69 = x67 
val x71 = x69.withColumn("odds", when(col("maleRatio") === 0.0, 0.0).otherwise(col("femaleRatio") / col("maleRatio"))).as[Record8925464e5ac642f2a363391f040e6c69]
 
val x72 = x71
val OddsRatio = x72
OddsRatio.collect.foreach(println(_))
//OddsRatio.cache
OddsRatio.count

  }

}

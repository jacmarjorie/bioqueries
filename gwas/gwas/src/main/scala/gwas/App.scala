package gwas

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang._

case class Record39053ab926fc407e815c45ed2408fad7(g_sample: String, call: Int)
case class Record762d6dbcdf3d4150916946ce0039acba(reference: String, genotypes: Seq[Record39053ab926fc407e815c45ed2408fad7], alternate: String, variants_index: Long, contig: String, start: Int)
case class Record228b25e5b1f146adabdd00c72d3a9690(reference: String, genotypes: Seq[Record39053ab926fc407e815c45ed2408fad7], alternate: String, variants_index: Long, contig: String, start: Int, genotypes_index: Long)
case class Record15d48b3c099f4fbea1fff83fe621c0aa(reference: String, g_sample: Option[String], alternate: String, variants_index: Long, contig: String, start: Int, call: Option[Int], genotypes_index: Long)
case class Record6729c2d1e4c74d21a772224c6885e19b(population: String, family_id: String, m_sample: String, metadata_index: Long, gender: String)
case class Record53cb3f32ef444ec68d073621182b7bff(reference: String, g_sample: Option[String], population: Option[String], family_id: Option[String], alternate: String, variants_index: Long, contig: String, m_sample: Option[String], metadata_index: Option[Long], start: Int, call: Option[Int], genotypes_index: Long, gender: Option[String])
case class Record572d99b2d934401d924f0d3db70b0020(reference: String, altCnt: Int, refCnt: Int, alternate: String, variants_index: Long, contig: String, start: Int, gender: Option[String])
case class Record186e29925ae8433bad5bd126c84cc85c(reference: String, altCnt: Double, refCnt: Double, alternate: String, variants_index: Long, contig: String, start: Int, gender: Option[String])
case class Recordd8b1eba210eb4851a8b734abf2f46f85(reference: String, alternate: String, variants_index: Long, contig: String, start: Int, gender: Option[String])
case class Record1e78f953abae4b798ec02361e1813586(reference: String, alternate: String, variants_index: Long, contig: String, start: Int)
case class Record64f0caaf5733475fa8ed0b918b7cbbbc(gender: String, refCnt: Double, altCnt: Double)
case class Record8accad78d9e24741b31178c4f1172985(reference: String, _2: Seq[Record64f0caaf5733475fa8ed0b918b7cbbbc], alternate: String, variants_index: Long, contig: String, start: Int)
case class Recordde3783cbb2014e939f05985d35315df7(gender: Option[String], refCnt: Double, altCnt: Double)
case class Recordcebceae21c6145e7b4c4d4ff0841f2ef(reference: String, calls: Seq[Recordde3783cbb2014e939f05985d35315df7], contig: String, alterante: String, start: Int)

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

 val x21 = variants.withColumn("variants_index", monotonically_increasing_id())
 .as[Record762d6dbcdf3d4150916946ce0039acba]
 
val x23 = x21 
val x24 = x23.withColumn("genotypes_index", monotonically_increasing_id())
 .as[Record228b25e5b1f146adabdd00c72d3a9690]
 
val x27 = x24.flatMap{
 case x25 => if (x25.genotypes.isEmpty) Seq(Record15d48b3c099f4fbea1fff83fe621c0aa(x25.reference, None, x25.alternate, x25.variants_index, x25.contig, x25.start, None, x25.genotypes_index))
   else x25.genotypes.map( x26 => Record15d48b3c099f4fbea1fff83fe621c0aa(x25.reference, Some(x26.g_sample), x25.alternate, x25.variants_index, x25.contig, x25.start, Some(x26.call), x25.genotypes_index) )
}.as[Record15d48b3c099f4fbea1fff83fe621c0aa]
 
val x28 = metadata.withColumn("metadata_index", monotonically_increasing_id())
 .as[Record6729c2d1e4c74d21a772224c6885e19b]
 
val x31 = x27.join(x28, 
 col("g_sample") === col("m_sample"), "left_outer").as[Record53cb3f32ef444ec68d073621182b7bff]
 
val x33 = x31.withColumn("altCnt", when(col("call") === 1, 1).otherwise(when(col("call") === 2, 2).otherwise(0)))
.withColumn("refCnt", when(col("call") === 1, 1).otherwise(when(col("call") === 2, 0).otherwise(2))).as[Record572d99b2d934401d924f0d3db70b0020]
 
val x35 = x33.groupByKey(x34 => Recordd8b1eba210eb4851a8b734abf2f46f85(x34.reference, x34.alternate, x34.variants_index, x34.contig, x34.start, x34.gender))
 .agg(typed.sum[Record572d99b2d934401d924f0d3db70b0020](x34 => x34.refCnt)
,typed.sum[Record572d99b2d934401d924f0d3db70b0020](x34 => x34.altCnt)
).mapPartitions{ it => it.map{ case (key, refCnt,altCnt) =>
   Record186e29925ae8433bad5bd126c84cc85c(key.reference,altCnt,refCnt,key.alternate,key.variants_index,key.contig,key.start,key.gender)
}}.as[Record186e29925ae8433bad5bd126c84cc85c]
 
val x37 = x35.groupByKey(x36 => Record1e78f953abae4b798ec02361e1813586(x36.reference, x36.alternate, x36.variants_index, x36.contig, x36.start)).mapGroups{
 case (key, value) => 
   val grp = value.flatMap(x36 => 
    x36.gender match {
      case None => Seq()
      case _ => Seq(Record64f0caaf5733475fa8ed0b918b7cbbbc(x36.gender.get, x36.refCnt, x36.altCnt))
   }).toSeq
   Record8accad78d9e24741b31178c4f1172985(key.reference, grp, key.alternate, key.variants_index, key.contig, key.start)
 }.as[Record8accad78d9e24741b31178c4f1172985]
 
val x39 = x37
// .withColumn("calls", x10._2)
// .withColumn("alterante", x10.alternate).as[Recordcebceae21c6145e7b4c4d4ff0841f2ef]
 
val x40 = x39
val OddsRatio = x40
OddsRatio.collect.foreach(println(_))
//OddsRatio.cache
// OddsRatio.count


  }

}
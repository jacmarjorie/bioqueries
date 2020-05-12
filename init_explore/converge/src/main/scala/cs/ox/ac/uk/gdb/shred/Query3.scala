package cs.ox.ac.uk.shred.test.converge

import org.apache.spark.sql.functions._
import com.oda.gdbspark.{GDBMapper, I2B2Loader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext.VariantContext
import org.apache.spark.sql.{Dataset, Row}
import java.io._

/**
  * Query3 joins variant and clincal data on patient identifiers, groups by clinical concept and value, and 
  * reports the genotype distribution. The clinical data in this query is derived from i2b2's crc cell, which
  * is a join of several tables.
  */
class Query3(spark_session: SparkSession, crcxml: String) extends Serializable{
  
  var get_skew = true
  var label = "converge"
  var outfile = "/mnt/app_hdd/scratch/flint-spark/shredding_q3.csv"
  var outfile2 = "/mnt/app_hdd/scratch/flint-spark/shredding_q3_partitions.csv"
  @transient val printer = new PrintWriter(new FileOutputStream(new File(outfile), true /* append = true */))
  @transient val printer2 = new PrintWriter(new FileOutputStream(new File(outfile2), true /* append = true */))

  import spark_session.sqlContext.implicits._

  // dataframe column manipulation helper functions
  val slice = udf((array : Seq[String], from : Int, to : Int) => array.slice(from,to))
  val mkString = udf((array : Seq[String]) => array.mkString(" ")+" ")

  val i2b2 = new I2B2Loader(spark_session.sparkContext, crcxml)
  val dbprops = "/usr/local/i2b2/wildfly-10.0.0.Final/db-properties.flat"
  val gdbmapI2b2 = new GDBMapper(spark_session, dbprops)
  val concepts = i2b2.conceptTable.withColumn("cpath", split($"concept_path", "\\\\"))
                        .withColumn("cpath2", slice($"cpath", lit(0), size($"cpath")-2))
                        .withColumn("concept_descr", mkString($"cpath2"))
                        .select("concept_path", "concept_cd", "name_char", "concept_descr").rdd.map(r =>
                           (r.getString(1), (r.getString(0), r.getString(2), r.getString(3))))
                        
  val observations = i2b2.observationTable.rdd.map(r => (r.getString(2), r.getString(1)))
  val patients = gdbmapI2b2.query("callset").rdd.map( r => (r.getString(2), r.getString(1)))
  val clin = observations.join(concepts).map{
    case (concept, (patient, (clin, value, grp))) => (patient, (concept, clin, value, grp))
  }.join(patients).map{
    case (patient, ((concept, clin, value, group), sample)) => (sample, (concept, value, group))
  }

  def unshred(flat: RDD[(String, Int, Long)], dict: RDD[(Long, List[(String, String, (Int, Int, Int, Int))])]) = {
    flat.map{ 
      case (contig, start, id) => id -> (contig, start) 
    }.join(dict).map{
      case (_, ((contig, start), genotypeDist)) => (contig, start, genotypeDist)
    }
  }

  def testFlat(region: Long, vs: RDD[VariantContext]): Unit = {
    
    if (get_skew){
      val p1 = vs.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q3_initial,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p1)
    }
    var start = System.currentTimeMillis()
    //flatten
    val rdd = vs.zipWithUniqueId
    val genotypes = rdd.map{
                      case (variant:VariantContext, id) => variant.getSampleNames.toList.map(sample =>
                        (sample, (variant.getContig, variant.getStart, id,
                          Utils.reportGenotypeType(variant.getGenotype(sample)))))
                    }.flatMap(g => g)

    val alleleCounts = genotypes.join(clin).map{
                        case (sample, ((contig, start, id, genotype), (concept, value, grp))) => 
                                                        ((contig, start, id, value, grp), genotype)
                       }
                      .combineByKey(
                        (genotype) => {
                          genotype match {
                            case 0 => (1, 0, 0, 0) //homref
                            case 1 => (0, 1, 0, 0) //het
                            case 2 => (0, 0, 1, 0) //homvar
                            case _ => (0, 0, 0, 0) //nocall
                        }},
                        (acc: (Int, Int, Int, Int), genotype) => {
                          genotype match {
                            case 0 => (acc._1 + 1, acc._2 + 0, acc._3 + 0, acc._4 + 0) //homref
                            case 1 => (acc._1 + 0, acc._2 + 1, acc._3 + 0, acc._4 + 0) //het
                            case 2 => (acc._1 + 0, acc._2 + 0, acc._3 + 1, acc._4 + 0) //homvar
                            case _ => (acc._1 + 0, acc._2 + 0, acc._3 + 0, acc._4 + 0) //nocall
                        }},
                        (acc1: (Int, Int, Int, Int), acc2: (Int, Int, Int, Int)) => {
                          (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3, acc1._4 + acc2._4)
                        })
    alleleCounts.count
    var end = System.currentTimeMillis() - start

    if (get_skew){
      val p2 = alleleCounts.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q3_flat,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p2)
    }

    printer.println(label+",q3_flat,"+region+","+end)
    printer.flush
    printer2.flush
  }

  def testShred(region: Long, vs: RDD[VariantContext]): Unit = {
    //shred
    var start = System.currentTimeMillis()
    val (v_flat, v_dict) = Utils.shred(vs)
    v_dict.cache
    v_dict.count
    v_flat.cache
    v_flat.count
    var end1 = System.currentTimeMillis() - start
    if (get_skew){
      val p3 = v_dict.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q3_shred,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //construct query
    var start2 = System.currentTimeMillis()
    val q3_flat = v_flat
    val q3_dict_1 = v_dict.flatMap{
        case (l, gg) => gg.map( g => g.getSampleName -> (l, Utils.reportGenotypeType(g)))
    }
    
    val q3_dict = q3_dict_1.join(clin).map{
        case (_, ((l, gt_call), (concept, value, group))) => (l, value, group) -> gt_call
    }.combineByKey(
          (genotype) => {
            genotype match {
              case 0 => (1, 0, 0, 0) //homref
              case 1 => (0, 1, 0, 0) //het
              case 2 => (0, 0, 1, 0) //homvar
              case _ => (0, 0, 0, 0) //nocall
          }},
          (acc: (Int, Int, Int, Int), genotype) => {
            genotype match {
              case 0 => (acc._1 + 1, acc._2 + 0, acc._3 + 0, acc._4 + 0) //homref
              case 1 => (acc._1 + 0, acc._2 + 1, acc._3 + 0, acc._4 + 0) //het
              case 2 => (acc._1 + 0, acc._2 + 0, acc._3 + 1, acc._4 + 0) //homvar
              case 3 => (acc._1 + 0, acc._2 + 0, acc._3 + 0, acc._4 + 0) //nocall
          }},
          (acc1: (Int, Int, Int, Int), acc2: (Int, Int, Int, Int)) => {
            (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3, acc1._4 + acc2._4)
          })
    .map{ case ((l,clin, value), agg) => l -> (clin, value, agg)}
    .mapPartitions(it => {
        it.toList.groupBy(_._1)
        .mapValues(_.map(_._2)).iterator
    }, true)
    q3_dict.cache
    q3_dict.count
    var end2 = System.currentTimeMillis() - start2

    if (get_skew){
      val p3 = q3_dict.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q3_shred_query,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //unshred
    var start3 = System.currentTimeMillis()
    val q3 = unshred(q3_flat, q3_dict)
    q3.cache
    q3.count
    var end3 = System.currentTimeMillis()
    var end = end3 - start
    var end4 = end3 - start3
    printer.println(label+",q3_shred,"+region+","+end1)
    printer.println(label+",q3_shred_query,"+region+","+end2)
    printer.println(label+",q3_unshred,"+region+","+end4)
    printer.println(label+",q3_shred_total,"+region+","+end)
    if (get_skew){
      val p4 = q3.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q3_unshred,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p4)
    }
    printer.flush
    printer2.flush
  }

  def close(): Unit = {
    printer.close()
    printer2.close()
  }
}

object Query3{
  def apply(spark_session: SparkSession, crcxml: String) = new Query3(spark_session, crcxml)
}

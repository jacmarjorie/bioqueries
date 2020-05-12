package cs.ox.ac.uk.shred.test.converge

import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext.VariantContext
import org.apache.spark.sql.{Dataset, Row}
import java.io._

/**
  * Query 1 joins variant and clinical data on patient identifier, then calculates allele count
  * for a binary clinical variable (has MDD diagnosis or not)
  */
object Query1{
  
  var get_skew = false
  var label = "converge"
  var outfile = "/mnt/app_hdd/scratch/flint-spark/shredding_q1.csv"
  var outfile2 = "/mnt/app_hdd/scratch/flint-spark/shredding_q1_partitions.csv"
  @transient val printer = new PrintWriter(new FileOutputStream(new File(outfile), true /* append = true */))
  @transient val printer2 = new PrintWriter(new FileOutputStream(new File(outfile2), true /* append = true */))

  def unshred(flat: RDD[(Long, (String, Int))], dict: RDD[((Long, Double), List[(Int, Int)])]) = {
    dict.map{
        case ((vid, iscase), alleleCounts) => (vid, (iscase, alleleCounts))
    }.join(flat).map{
        case (vid, ((iscase, alleleCounts), (contig, start))) => (contig, start, iscase, alleleCounts(0)._1, alleleCounts(0)._2)
    }
  }

  def testFlat(region: Long, vs: RDD[VariantContext], clin: Dataset[Row]): Unit = {
    
    var start = System.currentTimeMillis()
    //flatten, handle duplicate variant data
    val rdd = vs.zipWithUniqueId
    val genotypes = rdd.mapPartitions{ p => p.map{
                      case (variant:VariantContext, id) => variant.getSampleNames.toList.map(sample =>
                        (sample, (variant.getContig, variant.getStart, id,
                          Utils.reportGenotypeType(variant.getGenotype(sample)))))
                    }}.flatMap(g => g)
    val clinJoin = clin.select("id", "iscase").rdd.map(s => (s.getString(0), s.getDouble(1)))   
    genotypes.count 
    clinJoin.count
    var end1 = System.currentTimeMillis() - start
    //query on flatten
    val alleleCounts = genotypes.join(clinJoin).map{
                          case (sample, ((contig, start, vid, genotype), iscase)) =>
                                                  ((contig, start, vid, iscase), genotype)
                        }
                        .combineByKey(
                          (genotype) => {
                            genotype match {
                              case 0 => (2, 0) //homref
                              case 1 => (1, 1) //het
                              case 2 => (0, 2) //homvar
                              case _ => (0, 0) //nocall
                          }},
                          (acc: (Int, Int), genotype) => {
                            genotype match {
                              case 0 => (acc._1 + 2, acc._2 + 0) //homref
                              case 1 => (acc._1 + 1, acc._2 + 1) //het
                              case 2 => (acc._1 + 0, acc._2 + 2) //homvar
                              case _ => (acc._1 + 0, acc._2 + 0) //nocall
                          }},
                          (acc1: (Int, Int), acc2: (Int, Int)) => {
                            (acc1._1 + acc2._1, acc1._2 + acc2._2)
                          })
    alleleCounts.count
    var end = System.currentTimeMillis() - start

    printer.println(label+",q1_flat_flatten,"+region+","+end1)
    printer.println(label+",q1_flat_total,"+region+","+end)
    printer.flush
    printer2.flush
  }

  def testShred(region: Long, vs: RDD[VariantContext], clin: Dataset[Row]): Unit = {
    //shred
    var start = System.currentTimeMillis()
    val (v_flat, v_dict) = Utils.shred2(vs)
    v_dict.count
    var end1 = System.currentTimeMillis() - start
    var start1 = System.currentTimeMillis()
    //construct query
    val c_flat = clin.select("id", "iscase").rdd.map(s =>(s.getString(0), s.getDouble(1)))    
    val g_flat = v_dict.flatMap{
                    case (vid, genos) => genos.map{
                        case geno => (geno.getSampleName, (Utils.reportGenotypeType(geno), vid))
                    }
                  }
   
    val q1_dict = g_flat.join(c_flat).map{
                    case (sample, ((genotype, vid), iscase)) => (vid, iscase) -> genotype
    }.combineByKey(
      (genotype) => {
        genotype match {
          case 0 => (2, 0) //homref
          case 1 => (1, 1) //het
          case 2 => (0, 2) //homvar
          case _ => (0, 0) //nocall
      }},
      (acc: (Int, Int), genotype) => {
        genotype match {
          case 0 => (acc._1 + 2, acc._2 + 0) //homref
          case 1 => (acc._1 + 1, acc._2 + 1) //het
          case 2 => (acc._1 + 0, acc._2 + 2) //homvar
          case _ => (acc._1 + 0, acc._2 + 0) //nocall
      }},
      (acc1: (Int, Int), acc2: (Int, Int)) => {
        (acc1._1 + acc2._1, acc1._2 + acc2._2)
    })
    .mapPartitions(it => {
        it.toList.groupBy(_._1)
        .mapValues(_.map(_._2)).iterator
    }, true)
    q1_dict.count
    var end2 = System.currentTimeMillis() - start1   
    var start2 = System.currentTimeMillis()

    //unshred
    val q1 = unshred(v_flat, q1_dict)
    q1.count
    var end3 = System.currentTimeMillis() - start2
    var end = System.currentTimeMillis() - start
    printer.println(label+",q1_shred_shred,"+region+","+end1)
    printer.println(label+",q1_shred_query,"+region+","+end2)
    printer.println(label+",q1_shred_unshred,"+region+","+end3)
    printer.println(label+",q1_shred_total,"+region+","+end)
    printer.flush
    printer2.flush
  }

  def close(): Unit = {
    printer.close()
    printer2.close()
  }
}

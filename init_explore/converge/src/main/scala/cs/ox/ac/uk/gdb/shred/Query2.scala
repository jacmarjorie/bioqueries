package cs.ox.ac.uk.shred.test.converge

import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext.VariantContext
import org.apache.spark.sql.{Dataset, Row}
import java.io._


/**
  * Query2 joins variants and clinical data on patient identifier then computes
  * genotype distribution for a categorical variable
  */
object Query2{
  
  var get_skew = true
  var label = "converge"
  var outfile = "/mnt/app_hdd/scratch/flint-spark/shredding_q2.csv"
  var outfile2 = "/mnt/app_hdd/scratch/flint-spark/shredding_q2_partitions.csv"
  @transient val printer = new PrintWriter(new FileOutputStream(new File(outfile), true /* append = true */))
  @transient val printer2 = new PrintWriter(new FileOutputStream(new File(outfile2), true /* append = true */))

  def unshred(flat: RDD[(String, Int, Long)], dict: RDD[(Long, List[(Double, (Int, Int, Int, Int))])]) = {
    flat.map{ 
      case (contig, start, id) => id -> (contig, start)
    }.join(dict).map{
      case (_, ((contig, start),genotypeCnts)) => (contig, start, genotypeCnts)
    }
  }

  def testFlat(region: Long, vs: RDD[VariantContext], clin: Dataset[Row]): Unit = {
    
    if (get_skew){
      val p1 = vs.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q2_initial,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
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

    val clinical = clin.select("id", "cold_m").where("cold_m is not null").rdd.map{ row => 
                    (row.getString(0), row.getDouble(1))
                  }
  
    //query on flatten
    val alleleCounts = genotypes.join(clinical).map{
                          case (sample, ((contig, start, vid, genotype), iscase)) =>
                                                  ((contig, start, vid, iscase), genotype)
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
                          case _ => (acc._1 + 0, acc._2 + 0, acc._3 + 0, acc._4 + 1) //nocall
                      }},
                      (acc1: (Int, Int, Int, Int), acc2: (Int, Int, Int, Int)) => {
                        (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3, acc1._4 + acc2._4)
                      })
    alleleCounts.count
    var end = System.currentTimeMillis() - start

    if (get_skew){
      val p2 = alleleCounts.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q2_flat,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p2)
    }

    printer.println(label+",q2_flat,"+region+","+end)
    printer.flush
    printer2.flush
  }

  def testShred(region: Long, vs: RDD[VariantContext], clin: Dataset[Row]): Unit = {
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
        }.map(r => label+",q1_shred,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //construct query
    var start2 = System.currentTimeMillis()
    val q2_flat = v_flat
    val q2_dict_1 = v_dict.flatMap{
        case (l, gg) => gg.map( g => g.getSampleName -> (l, Utils.reportGenotypeType(g)))
    }
    
    val clinical = clin.select("id", "cold_m").where("cold_m is not null").rdd.map{
                      row => (row.getString(0), row.getDouble(1))
                    }

    val q2_dict = q2_dict_1.join(clinical).map{
        case (_, ((l, gt_call), coldm)) => (l, coldm) -> gt_call
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
              case _ => (acc._1 + 0, acc._2 + 0, acc._3 + 0, acc._4 + 1) //nocall
          }},
          (acc1: (Int, Int, Int, Int), acc2: (Int, Int, Int, Int)) => {
            (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3, acc1._4 + acc2._4)
          })
    .map{ case ((l,clin), agg) => l -> (clin, agg)}
    .mapPartitions(it => {
        it.toList.groupBy(_._1)
        .mapValues(_.map(_._2)).iterator
    }, true)
    q2_dict.cache
    q2_dict.count
    var end2 = System.currentTimeMillis() - start2

    if (get_skew){
      val p3 = q2_dict.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q2_shred_query,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //unshred
    var start3 = System.currentTimeMillis()
    val q2 = unshred(q2_flat, q2_dict)
    q2.cache
    q2.count
    var end3 = System.currentTimeMillis()
    var end = end3 - start
    var end4 = end3 - start3
    printer.println(label+",q2_shred,"+region+","+end1)
    printer.println(label+",q2_shred_query,"+region+","+end2)
    printer.println(label+",q2_unshred,"+region+","+end4)
    printer.println(label+",q2_shred_total,"+region+","+end)
    if (get_skew){
      val p4 = q2.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q2_unshred,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
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

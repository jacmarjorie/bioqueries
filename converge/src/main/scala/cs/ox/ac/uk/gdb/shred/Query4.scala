package cs.ox.ac.uk.shred.test.converge

import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext.VariantContext
import org.apache.spark.sql.{Dataset, Row}
import java.io._

/**
  * Query 4 joins variant and clinical data on patient identifier, then calculates allele count
  * for a binary clinical variable, uses that to determine the odds ratio. Odds ratio is kept if 
  * the value is not equal to 1.  
*/
object Query4{
  
  var get_skew = false
  var label = "converge"
  var outfile = "/mnt/app_hdd/scratch/flint-spark/shredding_q4.csv"
  var outfile2 = "/mnt/app_hdd/scratch/flint-spark/shredding_q4_partitions.csv"
  @transient val printer = new PrintWriter(new FileOutputStream(new File(outfile), true /* append = true */))
  @transient val printer2 = new PrintWriter(new FileOutputStream(new File(outfile2), true /* append = true */))

  def unshred(flat: RDD[(String, Int, Long)], dict: RDD[(Long, Double)]) = flat.map{ 
                                                                          case x => x._3 -> (x._1, x._2) }
                                                                          .join(dict).map{case (_, (x,y)) => 
                                                                                (x._1, x._2, y)}

  def testQ4(region: Long, vs: RDD[VariantContext], clin: Dataset[Row], snps: RDD[((String, Int), Int)]): Unit = {
    
    if (get_skew){
      val p1 = vs.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_initial,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p1)
    }
    var start = System.currentTimeMillis()
    //flatten
    val rdd = vs.zipWithUniqueId
    val genotypes = rdd.map( v => v._1.getSampleNames.toList.map(s => 
        (s, (v._1.getContig, v._1.getStart, v._2, Utils.reportGenotypeType(v._1.getGenotype(s)))))).flatMap(x => x)

    val clinJoin = clin.select("id", "iscase").rdd.map(s => (s.getString(0), s.getDouble(1)))   
 
    //query on flatten
    val oddsratio = genotypes.join(clinJoin)
                        .map( x => ((x._2._1._1, x._2._1._2, x._2._1._3, x._2._2), x._2._1._4))
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
                          }).map(x => 
                        (x._1, (x._2._1.toDouble/x._2._2.toDouble)))
                    .groupBy(x => (x._1._1, x._1._2)).map(x => {
                        val v = x._2.toList
                        if(v(0)._1._3 == 1.0){
                           (x._1, v(0)._2/v(1)._2)
                        }else{
                           (x._1, v(1)._2/v(0)._2)
                        }  
                    }).filter(x => x._2 != 1.00)//.join(snps)
    oddsratio.count
    var end = System.currentTimeMillis() - start


    if (get_skew){
      val p2 = oddsratio.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_flat,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p2)
    }

    printer.println(label+",q1_flat,"+region+","+end)
    printer.flush
    printer2.flush
  }

  def testQ4_shred(region: Long, vs: RDD[VariantContext], clin: Dataset[Row], snps: RDD[((String, Int), Int)]): Unit = {
    //shred
    var start = System.currentTimeMillis()
    val (v_flat, v_dict) = Utils.shred(vs)
    v_dict.cache
    v_dict.count
    v_flat.cache
    v_flat.count
    vs.unpersist()
    var end1 = System.currentTimeMillis() - start
    if (get_skew){
      val p3 = v_dict.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_shred,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //construct query
    var start2 = System.currentTimeMillis()
    val q1_flat = v_flat
    val q1_dict_1 = v_dict.flatMap{
        case (l, gg) => gg.map( g => g.getSampleName -> (l, Utils.reportGenotypeType(g)))
    }

    val q1_dict_2 = clin.select("id", "iscase").rdd.map(s =>(s.getString(0), s.getDouble(1)))
    
    val q1_dict = q1_dict_1.join(q1_dict_2).map{
        case (_, ((l, gt_call), clinAttr)) => (l, clinAttr) -> gt_call
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
      }).groupBy(x => x._1._1).map(x => {
        val d = x._2.toList
        if (d(0)._1._2 == 1.0){
          (x._1, (d(0)._2._1.toDouble/d(0)._2._2.toDouble)/(d(1)._2._1.toDouble/d(1)._2._2.toDouble))
      }else{
        (x._1, (d(1)._2._1.toDouble/d(1)._2._2.toDouble)/(d(0)._2._1.toDouble/d(0)._2._2.toDouble))
      }
    }).filter(x => x._2 != 1.00)
    q1_dict.count
    var end2 = System.currentTimeMillis() - start2

    if (get_skew){
      val p3 = q1_dict.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_shred_query,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //unshred
    var start3 = System.currentTimeMillis()
    val q1 = unshred(q1_flat, q1_dict)
    //q1.cache
    q1.count
    var end3 = System.currentTimeMillis()
    var end = end3 - start
    var end4 = end3 - start3
    printer.println(label+",q1_shred,"+region+","+end1)
    printer.println(label+",q1_shred_query,"+region+","+end2)
    printer.println(label+",q1_unshred,"+region+","+end4)
    printer.println(label+",q1_shred_total,"+region+","+end)
    if (get_skew){
      val p4 = q1.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_unshred,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
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

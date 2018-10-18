package cs.ox.ac.uk.shred.test.converge

import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext.VariantContext
import org.apache.spark.sql.{Dataset, Row}
import java.io._

/**
  * Query 4 joins variant and clinical data on patient identifier, then calculates allele count
  * for a binary clinical variable, uses that to determine the odds ratio.  
*/
object Query4{
  
  var get_skew = false
  var label = "converge"
  var outfile = "/mnt/app_hdd/scratch/flint-spark/shredding_q4.csv"
  var outfile2 = "/mnt/app_hdd/scratch/flint-spark/shredding_q4_partitions.csv"
  @transient val printer = new PrintWriter(new FileOutputStream(new File(outfile), true /* append = true */))
  @transient val printer2 = new PrintWriter(new FileOutputStream(new File(outfile2), true /* append = true */))

  def unshred(flat: RDD[(String, Int, Long)], dict: RDD[(Long, Double)]): RDD[(String, Int, Double)] = {
    flat.map{ 
      case (contig, start, id) => id -> (contig, start) 
    }.join(dict).map{
        case (_, ((contig, start), oddsratio )) => (contig, start, oddsratio)
    }
  }

  def testFlat(region: Long, vs: RDD[VariantContext], clin: Dataset[Row], snps: RDD[((String, Int), Int)]): Unit = {
    
    if (get_skew){
      val p1 = vs.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_initial,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
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

    val clinJoin = clin.select("id", "iscase").rdd.map(s => (s.getString(0), s.getDouble(1)))   
 
    val oddsratio = genotypes.join(clinJoin).map{
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
                          }).map{
                            case (id, (alt, ref)) =>
                                  (id, (alt.toDouble/ref))
                          }.groupBy{
                            case ((contig, start, _, _), _) => (contig, start)
                          }.map{
                            case (id, ratios) => ratios.toList match {
                              case List(((_,_,_,1.0), cse), ((_,_,_,0.0), cntrl)) => (id, cse/cntrl)
                              case List(((_,_,_,0.0), cse), ((_,_,_,1.0), cntrl)) => (id, cse/cntrl)
                              case _ => (id, 0.0)
                            }
                          }
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

  def testShred(region: Long, vs: RDD[VariantContext], clin: Dataset[Row], snps: RDD[((String, Int), Int)]): Unit = {
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
      }).groupBy{
        case ((id, iscase), _) => id
      }.map{
        case (id, ratios) => ratios.toList match {
          case List(((_, 1.0), (cseAlt,cseRef)), ((_, 0.0), (cntrlAlt, cntrlRef))) => 
                                (id, (cseAlt.toDouble/cseRef)/(cntrlAlt.toDouble/cntrlRef))
          case List(((_, 0.0), (cseAlt,cseRef)), ((_, 1.0), (cntrlAlt, cntrlRef))) => 
                                (id, (cseAlt.toDouble/cseRef)/(cntrlAlt.toDouble/cntrlRef))
      }
    }
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

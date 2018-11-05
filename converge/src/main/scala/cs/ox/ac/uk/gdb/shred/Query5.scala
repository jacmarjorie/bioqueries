package cs.ox.ac.uk.shred.test.converge

import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext.VariantContext
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.SparkSession
import java.io._

/**
  * Query 5 joins variant and clinical data on patient identifier, then calculates allele count
  * for a binary clinical variable, uses that to determine the odds ratio. 
  * Data is then mapped to a public variant idenfitier (dbSNP rs id), and joined with functional
  * annotations from Ensembl's VEP (Variant Effect Predictor) REST api (json format). 
*/
object Query5{
  
  var get_skew = false
  var label = "converge"
  var outfile = "/mnt/app_hdd/scratch/flint-spark/shredding_q5.csv"
  var outfile2 = "/mnt/app_hdd/scratch/flint-spark/shredding_q5_partitions.csv"
  @transient val printer = new PrintWriter(new FileOutputStream(new File(outfile), true /* append = true */))
  @transient val printer2 = new PrintWriter(new FileOutputStream(new File(outfile2), true /* append = true */))

  def unshred(flat: RDD[((String, Int), Long)], dict: RDD[(Long, (Double, String))]): RDD[(String, Int, String, Double)] = {
    flat.map{ 
      case ((contig, start), id) => id -> (contig, start) 
    }.join(dict).map{
        case (_, ((contig, start), (oddsratio, annot))) => (contig, start, annot, oddsratio)
    }
  }

  def testFlat(region: Long, vs: RDD[VariantContext], clin: Dataset[Row], snps: RDD[((String, Int), Int)], annots: RDD[(Int, org.apache.spark.sql.Row)]): Unit = {
    
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
                            case ((contig, start, id, _), _) => (contig, start, id)
                          }.map{
                            case (id, ratios) => ratios.toList match {
                              case List(((_,_,_,1.0), cse), ((_,_,_,0.0), cntrl)) => (id, cse/cntrl)
                              case List(((_,_,_,0.0), cse), ((_,_,_,1.0), cntrl)) => (id, cse/cntrl)
                              case _ => (id, 0.0)
                            }
                          }.map{
                            case ((contig, start, id), odds) => (contig, start) -> (id, odds) 
                          }.join(snps).map{
                            case ((contig, start), ((vid, odds), dbsnp)) => dbsnp -> (contig, start, vid, odds)
                          }.join(annots).map{
                            case (dbsnp, ((contig, start, vid, odds), annot)) => {
                              // This is going to work with nested attributes from the annotatoin API
                              (dbsnp, contig, start, vid, odds, annot.getAs[String]("most_severe_consequence"))
                            }
                          }.sortBy(_._5)
    oddsratio.count
    var end = System.currentTimeMillis() - start
    oddsratio.take(100).foreach(println)


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

  def testShred(region: Long, vs: RDD[VariantContext], clin: Dataset[Row], snps: RDD[((String, Int), Int)], annots: RDD[(Int, org.apache.spark.sql.Row)]): Unit = {
    
    val data = vs.map{ case v =>
                                ((v.getContig, v.getStart), v)
                }.join(snps).map{
                      case ((contig, start), (v, dbsnp)) => (dbsnp, v)
                }.join(annots)

    var start = System.currentTimeMillis()

    // shred variant data
    val lbl = data.zipWithUniqueId
    val v_flat = lbl.map{ 
                    case ((dbsnp, (variant, annot)), l) => (l, (variant.getContig, variant.getStart)) 
                  }
    val g_dict = lbl.map{ 
                    case ((dbsnp, (variant, annot)), l) => (l, variant.getGenotypesOrderedByName) 
                  }
    val a_dict = lbl.map{ 
                    case ((dbsnp, (variant, annot)), l) => 
                      (l, annot.getAs[List[org.apache.spark.sql.Row]]("transcript_consequence")) }
    v_flat.count
    g_dict.count
    a_dict.count
    vs.unpersist()
    
    // clinial data
    val c_flat = clin.select("id", "iscase").rdd.map(s =>(s.getString(0), s.getDouble(1)))
   
    var end1 = System.currentTimeMillis() - start
    if (get_skew){
      val p3 = g_dict.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_shred,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //construct query
    var start2 = System.currentTimeMillis()
    
    val g_flat = g_dict.flatMap{
        case (l, gg) => gg.map( g => g.getSampleName -> (l, Utils.reportGenotypeType(g)))
    }
    
    val q1_dict = g_flat.join(c_flat).map{
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
      }).map{
        case ((vid, iscase), (ref, alt)) => vid -> (iscase, alt.toDouble/ref)
      }.reduceByKey{
        case ((1.0, ratioAlt), (0.0, ratioRef)) =>
            (ratioAlt/ratioRef, 1.0)
        case ((0.0, ratioRef), (1.0, ratioAlt)) =>
            (ratioAlt/ratioRef, 1.0)
      }.mapPartitions(it => {
        it.toList.groupBy(_._1)
        .mapValues(_.map(_._2)).iterator
      }, true)
    q1_dict.count

    val a_flat = a_dict.flatMap{
      case (aid, annot) => annot.map{
        case conseq => Utils.parseAnnot(aid, conseq)}
    }

    var end2 = System.currentTimeMillis() - start2

    if (get_skew){
      val p3 = q1_dict.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_shred_query,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //unshred
    var start3 = System.currentTimeMillis()
    val q1 = q1_dict.join(a_flat).join(v_flat).map{
        case (_, ((oddsratio, annot), (contig, start))) => (contig, start, annot, oddsratio)
    }
    //q1.cache
    q1.count
  
    var end3 = System.currentTimeMillis()
    var end = end3 - start
    var end4 = end3 - start3
    q1.take(100).foreach(println)
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


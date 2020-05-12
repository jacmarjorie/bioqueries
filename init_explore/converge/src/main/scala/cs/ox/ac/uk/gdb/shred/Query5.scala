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
    
    val data = vs.map{ case v =>
                                ((v.getContig, v.getStart), v)
                }.join(snps).map{
                      case ((contig, start), (v, dbsnp)) => (dbsnp, v)
                }.join(annots)
    data.count
    var start = System.currentTimeMillis()  
    val lbl = data.zipWithUniqueId
    val g_flat = lbl.flatMap{
      case ((dbsnp, (variant:VariantContext, annot)), id) => variant.getGenotypesOrderedByName.flatMap{
        geno => geno.getGenotypeString.split("/").map{
            g => ((variant.getContig, variant.getStart, 
                  variant.getAlleles.filter(_.isReference).map(_.getBaseString).toList(0), 
                  variant.getAlleles.filter(!_.isReference).map(_.getBaseString).toList, g), 
                  (geno.getSampleName, Utils.reportGenotypeType(geno), id))
            }
        }}
    
    val a_flat = lbl.flatMap{
      case ((dbsnp, (variant, annot)), l) => try {
            Utils.parseAnnotFlat(variant, annot.getAs[Seq[org.apache.spark.sql.Row]]("transcript_consequences"))
        }catch{
            case e:Exception => try {
                 Utils.parseAnnotFlat(variant, annot.getAs[Seq[org.apache.spark.sql.Row]]("regulatory_feature_consequences"))
            }catch{
                case e: Exception =>  try{
                 Utils.parseAnnotFlat(variant, annot.getAs[Seq[org.apache.spark.sql.Row]]("intergenic_consequences"))
                }catch{
                    case e: Exception => 
                        Utils.parseAnnotFlat(variant, Seq[org.apache.spark.sql.Row]())
            }
        }
      }
    }

    g_flat.count
    a_flat.count
    val c_flat = clin.select("id", "iscase").rdd.map(s => (s.getString(0), s.getDouble(1)))   
    var end1 = System.currentTimeMillis() - start
    var start1 = System.currentTimeMillis()

    val q1_dict = g_flat.join(a_flat.filter(_._2 != null)).map{
      case ((contig, start, ref, alts, allele), 
            ((sample, call, dbsnp), (conseq, biotype, impact, gene, hgnc, trans, symb))) => 
              ((contig, start, ref, alts, conseq), 1)
      }.reduceByKey(_ + _).map{
        case ((contig, start, ref, alts, conseq), cnt) => (contig, start, ref, alts) -> (conseq, cnt)
      }

    val q1_dict2 = g_flat.map{
      case ((contig, start, ref, alts, allele), (sample, geno, dbsnp)) => 
        (sample, (contig, start, ref, alts, allele, geno))
      }.join(c_flat).map{
        case (sample, ((contig, start, ref, alts, allele, geno), iscase)) => (contig, start, ref, alts, iscase) -> geno
      }.combineByKey(
        (genotype:Int) => {
          genotype match {
            case 0 => (2, 0) //homref
            case 1 => (1, 1) //het
            case 2 => (0, 2) //homvar
            case _ => (0, 0) //nocall
          }},
          (acc: (Int, Int), genotype:Int) => {
          genotype match {
            case 0 => (acc._1 + 2, acc._2 + 0) //homref
            case 1 => (acc._1 + 1, acc._2 + 1) //het
            case 2 => (acc._1 + 0, acc._2 + 2) //homvar
            case _ => (acc._1 + 0, acc._2 + 0) //nocall
          }},
          (acc1: (Int, Int), acc2: (Int, Int)) => {
            (acc1._1 + acc2._1, acc1._2 + acc2._2)
          })
          .map{
            case ((contig, start, ref, alts, iscase), (alt, refCnt)) => 
              ((contig, start, ref, alts), (iscase, alt, refCnt))
          }

      val results = q1_dict.join(q1_dict2)
      results.count
      var end2 = System.currentTimeMillis() - start1
      var end = System.currentTimeMillis() - start
      printer.println(label+",q1_flat_flatten,"+region+","+end1)
      printer.println(label+",q1_flat_query,"+region+","+end2)
      printer.println(label+",q1_flat_total,"+region+","+end)
      printer.flush
      printer2.flush
  }
  
  def testShred(region: Long, vs: RDD[VariantContext], clin: Dataset[Row], snps: RDD[((String, Int), Int)], annots: RDD[(Int, org.apache.spark.sql.Row)]): Unit = {
    
    val data = vs.map{ case v => // 114
                                ((v.getContig, v.getStart), v)
                }.join(snps).map{ // 116
                      case ((contig, start), (v, dbsnp)) => (dbsnp, v)
                }.join(annots)
    data.count
    var start = System.currentTimeMillis()

    // shred variant data
    val lbl = data.zipWithUniqueId
    val v_flat = lbl.map{
                case ((dbsnp, (variant, annot)), l) => (l, (variant.getContig, variant.getStart, variant.getAlleles.filter(_.isReference).map(_.getBaseString).toList(0), variant.getAlleles.filter(!_.isReference).map(_.getBaseString).toList))
              }
    val g_dict = lbl.map{
                case ((dbsnp, (variant, annot)), l) => (l, variant.getGenotypesOrderedByName)
              }
    val a_dict = lbl.map{
                case ((dbsnp, (variant, annot)), l) => try {
                        (l, annot.getAs[Seq[org.apache.spark.sql.Row]]("transcript_consequences")) 
                    }catch{
                        case e:Exception => try {
                            (l, annot.getAs[Seq[org.apache.spark.sql.Row]]("regulatory_feature_consequences"))
                        }catch{
                            case e: Exception =>  try{
                                (l, annot.getAs[Seq[org.apache.spark.sql.Row]]("intergenic_consequences"))
                            }catch{
                                case e: Exception => (l, Seq[org.apache.spark.sql.Row]())
                            }
                        }
                    }
                }
    g_dict.count //145
    a_dict.count //146
    var end1 = System.currentTimeMillis() - start
    var start1 = System.currentTimeMillis() 
   
    // clinial data
    val c_flat = clin.select("id", "iscase").rdd.map(s =>(s.getString(0), s.getDouble(1)))
   
    
    //construct query

    val a_flat = a_dict.filter(_._2 != null).flatMap{ //154
      case (aid, annot) => annot.flatMap{
      case conseq => Utils.parseAnnot(aid, conseq)}
    }
    
    val g_flat = g_dict.flatMap{ //159
      case (l, gg) => gg.flatMap{
        g => g.getGenotypeString().split("/").map{
            s => ((l, s), (g.getSampleName, Utils.reportGenotypeType(g)))
          }
        }
      }
  
    val q1_dict1 = g_flat.join(a_flat).map{ //167
                    case ((vid, allele), (sample, (conseq, biotype, impact, gene, hgnc, trans, symb))) => 
                        ((vid, conseq), 1)
                    }.reduceByKey(_ + _).map{
                      case ((vid, conseq), cnt) => vid -> (conseq, cnt)
                    }

    val q1_dict2 = g_flat.map{
        case ((l, a), (s, g)) => (s, (l,a,g))
      }.join(c_flat).map{
        case (s, ((v, a, g), iscase)) => (v, iscase) -> g
      }
    .combineByKey(
    (genotype:Int) => {
      genotype match {
        case 0 => (2, 0) //homref
        case 1 => (1, 1) //het
        case 2 => (0, 2) //homvar
        case _ => (0, 0) //nocall
      }},
      (acc: (Int, Int), genotype:Int) => {
      genotype match {
        case 0 => (acc._1 + 2, acc._2 + 0) //homref
        case 1 => (acc._1 + 1, acc._2 + 1) //het
        case 2 => (acc._1 + 0, acc._2 + 2) //homvar
        case _ => (acc._1 + 0, acc._2 + 0) //nocall
      }},
      (acc1: (Int, Int), acc2: (Int, Int)) => {
        (acc1._1 + acc2._1, acc1._2 + acc2._2)
      }).map{
        case ((l, iscase), (alt, ref)) => (l, (iscase, alt, ref))
      }
      .mapPartitions(it => {
        it.toList.groupBy(_._1)
        .mapValues(_.map(_._2)).iterator
      }, true)
       
    q1_dict1.count
    q1_dict2.count
    var end2 = System.currentTimeMillis() - start1
    var start2 = System.currentTimeMillis()
    //unshred 
    val result = q1_dict1.join(q1_dict2).join(v_flat)
            .map{
                case (vid, (((annot, cnt), alleleCnts),(contig, start, ref, alts))) => 
                    (contig, start, ref, alts, annot, cnt, alleleCnts)
              } 
    result.count
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


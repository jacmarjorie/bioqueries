package cs.ox.ac.uk.shred.test.converge

//import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import org.apache.spark.sql.Row
import htsjdk.variant.variantcontext._

object Calc{

  def oddsratioShred(g_flat: RDD[(String, (Int, Long))], c_flat: RDD[(String, String)]): RDD[(Long, List[(String, Double)])] = {
    g_flat.join(c_flat).map{
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
      }).map{
        case ((vid, iscase), (ref, alt)) => vid -> (iscase, alt.toDouble/ref)
      }.reduceByKey{
        case (("1.0", ratioAlt), ("0.0", ratioRef)) =>
            ("1.0", ratioAlt/ratioRef)
        case (("0.0", ratioRef), ("1.0", ratioAlt)) =>
            ("1.0", ratioAlt/ratioRef)
      }.mapPartitions(it => {
        it.toList.groupBy(_._1)
        .mapValues(_.map(_._2)).iterator
      }, true)
  }

  
  /**
    * Calculate oddsratio for a binary clinical variable
    * Clinical variable should be encoded as a String
    */
  def oddsratio(genotypes: RDD[(String, (String, Int, Long, Int))], clinical: RDD[(String, String)]): RDD[((String, Int, Long), Any)] = {
    genotypes.join(clinical).map{
        case (sample, ((contig, start, vid, genotype), clin)) =>
                                ((contig, start, vid, clin), genotype)
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
            case List(((_,_,_,"1.0"), cse), ((_,_,_,"0.0"), cntrl)) => (id, cse/cntrl)
            case List(((_,_,_,"0.0"), cse), ((_,_,_,"1.0"), cntrl)) => (id, cse/cntrl)
            case _ => (id, "0.0")
          }
        }
    }
  
  def annotate(v: (Int, (VariantContext, Map[String,List[Map[String, String]]]))) = v match {
    case (dbsnp, (variant, annot)) => {
        variant.getGenotypesOrderedByName.map{ geno =>
            val a = try {
                    Utils.parseAnnotFlat(variant, annot("transcript_consequences"))
                }catch{
                    case e:Exception => try {
                        Utils.parseAnnotFlat(variant,
                         annot("regulatory_feature_consequences"))
                    }catch{
                        case e: Exception =>  try{
                            Utils.parseAnnotFlat(variant, annot("intergenic_consequences"))
                        }catch{
                            case e: Exception =>
                                Utils.parseAnnotFlat(variant, List[Map[String,String]]())
                            }
                        }
                    }
            geno.getGenotypeString.split("/").map{ g =>
                val g2 = ((variant.getContig, variant.getStart,
                  variant.getAlleles.filter(_.isReference).map(_.getBaseString).toList(0),
                  variant.getAlleles.filter(!_.isReference).map(_.getBaseString).toList, g),
                  (geno.getSampleName, Utils.reportGenotypeType(geno)))
                (g2._1, g2._2, a.filter{ case (id, _) => id == g2._1})
            }
        }
    }   
  }

}


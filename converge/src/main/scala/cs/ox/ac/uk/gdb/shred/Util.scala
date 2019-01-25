package cs.ox.ac.uk.shred.test.converge

import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext._

object Utils extends Serializable{

    def reportGenotypeType(gt: Genotype): Int = {
      gt match {
        case gt if gt.isNoCall => 0;
        //case gt if gt.isHomRef => 0;
        case gt if gt.isHet => 1;
        case gt if gt.isHomVar => 2;
        //case gt if gt.isNoCall => 3;
      }
    }

    def flattenVariants(vs: RDD[VariantContext]): RDD[(String, (String, Int, Long, Int))] = {
      vs.zipWithUniqueId.mapPartitions{ p => p.map{
            case (variant:VariantContext, id) => variant.getSampleNames.toList.map(sample =>
                (sample, (variant.getContig, variant.getStart, id,
                          reportGenotypeType(variant.getGenotype(sample)))))
            }}.flatMap(g => g)
    }

    def flattenShredVariants(v_dict: RDD[(Long, java.lang.Iterable[htsjdk.variant.variantcontext.Genotype])]): RDD[(String, (Int, Long))] = {
      v_dict.flatMap{
                    case (vid, genos) => genos.map{
                        case geno => (geno.getSampleName, (Utils.reportGenotypeType(geno), vid))
                    }
                  }
    }
  
    /**
      * The shredding here is taking VariantContext(contig: String, start: Int, genotypes: List[Genotype])
      * Into a flat: (l, 
     */
    def shred(rdd: RDD[VariantContext]) = {
      val lbl = rdd.zipWithUniqueId
      val flat = lbl.map( i => i match { case (x,l) => (x.getContig, x.getStart, l) })
      val dict = lbl.map( i => i match { case (x,l) => (l, x.getGenotypesOrderedByName) })
      (flat,dict)
    }

    def shred2(rdd: RDD[VariantContext]) = {
      val lbl = rdd.zipWithUniqueId
      val flat = lbl.mapPartitions{ p => p.map{ case (x,l) => (l, (x.getContig, x.getStart))}}
      val dict = lbl.mapPartitions{ p => p.map{ case (x,l) => (l, x.getGenotypesOrderedByName)}}
      (flat,dict)
    }

    def shredSave(rdd: RDD[VariantContext]) = {
      val lbl = rdd.zipWithUniqueId
      val flat = lbl.mapPartitions{ p => p.map{ case (x,l) => (l, (x.getContig, x.getStart))}}
      val dict = lbl.mapPartitions{ p => p.map{ case (x:VariantContext,l:Long) => (l, x.getGenotypesOrderedByName.map{
                      case genotype => (genotype.getSampleName, (reportGenotypeType(genotype)))
                    })
                 }}
      (flat,dict)
    }

  def parseAnnot(aid: Long, conseq: Map[String,String]) = {
    conseq("consequence_terms").asInstanceOf[List[String]].map{
      c => ((aid, conseq("variant_allele")),
        (c,
        conseq("biotype"),
        conseq("impact"),
        conseq("gene_id"),
        conseq("hgnc_id"),
        conseq("transcript_id"),
        conseq("gene_symbol"))
      )
    }
  }

  def parseAnnotFlat(variant: VariantContext, annot: List[Map[String,String]]) = {
    annot.flatMap{
        conseq => conseq("consequence_terms").asInstanceOf[List[String]].map{
          c => ((variant.getContig, variant.getStart,
                  variant.getAlleles.filter(_.isReference).map(_.getBaseString).toList(0),
                  variant.getAlleles.filter(!_.isReference).map(_.getBaseString).toList,
                  conseq("variant_allele")),
              (c,
               conseq("biotype"),
               conseq("impact"),
               conseq("gene_id"),
               conseq("hgnc_id"),
               conseq("transcript_id"),
               conseq("gene_symbol")))
            }
          }
        }
}


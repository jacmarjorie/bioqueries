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


  
    /**
      * The shredding here is taking VariantContext(contig: String, start: Int, genotypes: List[Genotype])
      * Into a flat: (l, 
     */
    def shred(rdd: RDD[VariantContext]) = {
      val lbl = rdd.zipWithUniqueId
      val flat = lbl.map( i => i match { case (x,l) => (x.getContig, x.getStart, l) })
      // dict is of type RDD[(long, List[Genotype])]
      // how is this any different than RDD[VariantContext], since the second value is
      // still a nested list
      val dict = lbl.map( i => i match { case (x,l) => (l, x.getGenotypesOrderedByName) })
      (flat,dict)
    }

    def shred2(rdd: RDD[VariantContext]) = {
      val lbl = rdd.zipWithUniqueId
      val flat = lbl.map( i => i match { case (x,l) => (l, (x.getContig, x.getStart)) })
      val dict = lbl.map( i => i match { case (x,l) => (l, x.getGenotypesOrderedByName) })
      (flat,dict)
    }

    /**
      * This was an effort to the issues I list in shred() above
      * key is optimized for the clinical join
     */
    def shred3(rdd: RDD[VariantContext]) = {
      val lbl = rdd.zipWithUniqueId
      val flat = lbl.map{ case (variant,vpk) => (vpk, (variant.getContig, variant.getStart)) }
      // what does a full shred of the genotype data look like, 
      // note that i defind the output type to join nicely with the clinical set (on sampleName)
      // but i could have still keyed this by the variant label
      val dict = lbl.flatMap{ case (variant, vpk) => variant.getGenotypesOrderedByName.map{
                      case genotype => (genotype.getSampleName, (reportGenotypeType(genotype), vpk))
                    }
                 }
      (flat,dict)
    }

    /**
      * This was an effort to the issues I list in shred() above
      * however, it keeps the index on variant
     */
    def shred4(rdd: RDD[VariantContext]) = {
      val lbl = rdd.zipWithUniqueId
      val flat = lbl.map{ case (variant,vpk) => (vpk, (variant.getContig, variant.getStart)) }
      // what does a full shred of the genotype data look like, 
      // note that i defind the output type to join nicely with the clinical set (on sampleName)
      // but i could have still keyed this by the variant label
      val dict = lbl.flatMap{ case (variant, vpk) => variant.getGenotypesOrderedByName.map{
                      case genotype => (vpk, (genotype.getSampleName, reportGenotypeType(genotype)))
                    }
                 }
      (flat,dict)
    }

    def parseAnnot(aid: Long, conseq: org.apache.spark.sql.Row) = {
      (aid, (
        conseq.getAs[String]("biotype"), 
        conseq.getAs[Seq[String]]("consequence_terms"), 
        conseq.getAs[String]("impact"), 
        conseq.getAs[String]("gene_id"), 
        conseq.getAs[String]("hgnc_id"), 
        conseq.getAs[String]("transcript_id"), 
        conseq.getAs[String]("variant_allele"), 
        conseq.getAs[String]("gene_symbol"))
      )
    } 
}


package burden

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.scalalang._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.LongWritable
import htsjdk.variant.variantcontext.{CommonInfo, VariantContext}
import org.seqdoop.hadoop_bam.{VCFInputFormat, VariantContextWritable}

// todo add the other attributes
case class GeneInfo(contig: String, name: String, id: String, strand: String, affect: String)
case class VariantWithGene(contig: String, start: Int, info: List[GeneInfo])
case class FlatVariant(contig: String, start: Int, gene: String)
case class GeneCnt(name: String, cnt: Int)
case class Gene(name: String)
case class Pathway(name: String, url: String, gene_set: List[Gene])
case class FlatPathway(name: String, p_gene: String)
case class VariantPathway(contig: String, start: Int, name: String, gene: String, p_gene: String)

object App {
  
  def main(args: Array[String]){

   // standard setup
   val conf = new SparkConf().setMaster("local[*]")
     .setAppName("GeneBurden")
   val spark = SparkSession.builder().config(conf).getOrCreate()
   import spark.implicits._

   def loadVCF(path: String): RDD[VariantContext] = {
     spark.sparkContext
      .newAPIHadoopFile[LongWritable, VariantContextWritable, VCFInputFormat](path)
      .map(_._2.get)
   }

   // update this with your info
   val basepath = "/Users/jac/bioqueries/data/"
   val variants = loadVCF(basepath+"ALL.wgs.integrated_phase1_release_v3_coding_annotation.20101123.snps_indels.sites.vcf")
  
   // map the variants and get the gene from the info field using the htsjdk api
   // these changes the RDD[VariantWithGene] to a Dataframe 
   // and .as casts it to a Dataset[VariantWithGene]
   // define a function to extract the gene
   def getGene(v: VariantContext): GeneInfo = {
      val info = v.getCommonInfo.getAttributeAsString("VA", "").split(":")
      GeneInfo(info(0), info(1), info(2), info(3), info(4))
   }
   val subvcf = variants.map(v => VariantWithGene(v.getContig, v.getStart, List(getGene(v)))).toDF.as[VariantWithGene]

   // now do subtask two, as discussed the NRC is:
   //
   // subtask1 <== for v in VCF union
   //    for g in v.info union
   //        { < name := g.name, cnt := 1 > }
   //
   // result <== sumBy^{cnt}_{name}(subtask1)
   //
   // we can translate this to spark as
   val subtask1 = subvcf.flatMap(v =>         // for v in VCF union
      v.info.map(g =>                         //    for g in v.info union
          GeneCnt(g.name, 1)                  //      {< g.name, 1 >}
        )
    ).as[GeneCnt]                             // cast to Dataset 

   val result = subtask1.groupByKey(x =>        // sumBy^{cnt}_{name}(subtask1)
    x.name).agg(typed.sum[GeneCnt](x => x.cnt))

   result.take(10).foreach(println(_))

   // now reading in a file into a nested object
   // ie. the pathway information
   // by loading this way you can avoid explicitly specifying a schema
   // for standard csv files, I usually specify a schema
   // you can see that here: 
   // https://github.com/jacmarjorie/bioqueries/blob/gwas/gwas/gwas/src/main/scala/gwas/ClinicalLoader.scala
   val pathways = spark.sparkContext.textFile(basepath+"c2.all.v7.1.entrez.gmt").map(
      line => {
        val sline = line.split("\t")
        Pathway(sline(0), sline(1), sline.drop(2).map(g => Gene(g)).toList)
      }
   ).toDF.as[Pathway]

   // to combine the variant and pathway information, we want to use the dataset api -
   // check out the spark api docs:
   // https://spark.apache.org/docs/2.4.2/api/scala/index.html#org.apache.spark.sql.Dataset
   // join on the pathway information
   // for v in VCF union
   //    for g in v.info union                         // flatMap
   //        for p in Pathways union 
   //           for g2 in p.gene_set union             // flatMap
   //              if g.name == g2.name                // join
   //                 ... finish this
   // since both are nested both have to be flattened
   val variantsFlat = subvcf.flatMap(v => v.info.map(g => FlatVariant(v.contig, v.start, g.name))).as[FlatVariant]
   val pathwayFlat = pathways.flatMap(p => p.gene_set.map(g => FlatPathway(p.name, g.name))).as[FlatPathway]

   val joined = variantsFlat.join(pathwayFlat, $"gene" === $"p_gene").as[VariantPathway]

   joined.take(10).foreach(println(_))

   spark.stop()


  }
}


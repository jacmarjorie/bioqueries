package gwas

import com.oda.gdbspark._
import org.apache.spark.rdd.RDD
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import htsjdk.variant.variantcontext.VariantContext

class VariantLoader(spark: SparkSession) extends Serializable {

  val gdbmap = new GDBMapper(spark, Some(customConf))
  val gdb = new GDBConnector(spark, gdbmap, Config.workspace)

  def getVariants(samples: List[String], genes: List[String]): RDD[VariantContext] = {
    gdb.queryByGene(samples, genes).map(_._2)
  }

}
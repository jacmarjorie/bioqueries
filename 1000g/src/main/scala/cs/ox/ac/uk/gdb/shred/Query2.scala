package cs.ox.ac.uk.shred.test.onekg

import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext.VariantContext
import org.apache.spark.sql.{Dataset, SparkSession, Row}
import scala.collection.mutable.ListBuffer

/**
  * Query2 joins variants and clinical data on patient identifier then computes
  * genotype distribution for a categorical variable
  */
class Query2(spark_session: SparkSession) extends Serializable{
  
  var get_skew = true
  var label = "1000g"

  val result = new ListBuffer[(String, String, Long, Long)]()
  val result2 = new ListBuffer[(String, String, Long, Int, Int)]()

  def unshred(flat: RDD[(String, Int, Long)], dict: RDD[(Long, List[(String, (Int, Int, Int, Int))])]) = flat.map{ 
                                                                          case x => x._3 -> (x._1, x._2) }
                                                                          .join(dict).map{case (_, (x,y)) => 
                                                                                (x._1, x._2, y)}

  def testQ2(region: Long, vs: RDD[VariantContext], clin: Dataset[Row]): Unit = {
    
    if (get_skew){
      val p1 = vs.mapPartitionsWithIndex{
            case (i,rows) => Iterator((label,"q2_initial",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p1
    }
    var start = System.currentTimeMillis()
    
    //flatten
    val genotypes = vs.map( v => v.getSampleNames.toList.map(s =>
        (s, (v.getContig, v.getStart, Utils.reportGenotypeType(v.getGenotype(s)))))).flatMap(x => x)    
    val clinical = clin.select("individual_id", "population").rdd.map(s => (s.getString(0), s.getString(1)))
  
    //query on flatten
    val alleleCounts = genotypes.join(clinical)
                    .map( x => ((x._2._1._1, x._2._1._2, x._2._2), x._2._1._3))
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
            case (i,rows) => Iterator((label,"q2_flat",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p2
    }
    result ++= Seq((label,"q2_flat",region,end))
  }

  def testQ2_shred(region: Long, vs: RDD[VariantContext], clin: Dataset[Row]): Unit = {
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
            case (i,rows) => Iterator((label,"q2_shred",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p3
    }
    
    //construct query
    var start2 = System.currentTimeMillis()
    val q2_flat = v_flat
    val q2_dict_1 = v_dict.flatMap{
        case (l, gg) => gg.map( g => g.getSampleName -> (l, Utils.reportGenotypeType(g)))
    }
    
    val clinical = clin.select("individual_id", "population").rdd.map(s => (s.getString(0), s.getString(1)))
    val q2_dict = q2_dict_1.join(clinical).map{
        case (_, ((l, gt_call), population)) => (l, population) -> gt_call
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
            case (i,rows) => Iterator((label,"q2_shred_query",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p3
    }
    
    //unshred
    var start3 = System.currentTimeMillis()
    val q2 = unshred(q2_flat, q2_dict)
    q2.cache
    q2.count
    var end3 = System.currentTimeMillis()
    var end = end3 - start
    var end4 = end3 - start3
    result ++= Seq((label,"q2_shred",region,end1))
    result ++= Seq((label,"q2_shred_query",region,end2))
    result ++= Seq((label,"q2_unshred",region,end4))
    result ++= Seq((label,"q2_shred_total",region,end))
    if (get_skew){
      val p4 = q2.mapPartitionsWithIndex{
            case (i,rows) => Iterator((label,"q2_unshred",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p4
    }
  }
  
  def writeResult(): Unit = {
    Utils.writeResult(spark_session, result.toList, result2.toList, "q2", get_skew)
  }  

}

object Query2{
  def apply(spark_session: SparkSession) = new Query2(spark_session)
}

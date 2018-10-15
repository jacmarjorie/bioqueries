package cs.ox.ac.uk.shred.test.onekg

import org.apache.spark.rdd.RDD
import collection.JavaConversions._
import htsjdk.variant.variantcontext.VariantContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession, Row}
import scala.collection.mutable.ListBuffer

/**
  * Query 1 joins variant and clinical data on patient identifier, then calculates allele count
  * for a binary clinical variable
  */
class Query1Broadcast(spark_session: SparkSession) extends Serializable{
  
  var get_skew = false
  var label = "1000g"

  val result = new ListBuffer[(String, String, Long, Long)]()
  val result2 = new ListBuffer[(String, String, Long, Int, Int)]()

  def unshred(flat: RDD[(String, Int, Long)], dict: RDD[(Long, List[(Int, (Int, Int))])]) = flat.map{ 
                                                                          case x => x._3 -> (x._1, x._2) }
                                                                          .join(dict).map{case (_, (x,y)) => 
                                                                                (x._1, x._2, y)}

  def testQ1(region: Long, vs: RDD[VariantContext], clin: Broadcast[Dataset[Row]]): Unit = {
    
    if (get_skew){
      val p1 = vs.mapPartitionsWithIndex{
            case (i,rows) => Iterator((label,"q1bc_initial",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p1 
    }
    var start = System.currentTimeMillis()
    //flatten
    val genotypes = vs.map( v => v.getSampleNames.toList.map(s => 
        (s, (v.getContig, v.getStart, Utils.reportGenotypeType(v.getGenotype(s)))))).flatMap(x => x)

    val clinical = clin.value.rdd.map(s => (s.getString(1), s.getInt(4)))
    val alleleCounts = genotypes.join(clinical)
                        .map( x => ((x._2._1._1, x._2._1._2, x._2._2), x._2._1._3))
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
                          })
    alleleCounts.count
    var end = System.currentTimeMillis() - start

    if (get_skew){
      val p2 = alleleCounts.mapPartitionsWithIndex{
            case (i,rows) => Iterator((label,"q1bc_flat",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p2
    }
    result ++= Seq((label,"q1bc_flat",region,end))
  }

  def testQ1_shred(region: Long, vs: RDD[VariantContext], clin: Broadcast[Dataset[Row]]): Unit = {
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
            case (i,rows) => Iterator((label,"q1bc_shred",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p3
    }
    
    //construct query
    var start2 = System.currentTimeMillis()
    val q1_flat = v_flat
    val q1_dict_1 = v_dict.flatMap{
        case (l, gg) => gg.map( g => g.getSampleName -> (l, Utils.reportGenotypeType(g)))
    }

    val q1_dict_2 = clin.value.rdd.map {
        c => (c.getString(1), c.getInt(4))
    }

    val q1_dict = q1_dict_1.join(q1_dict_2).map{
        case (_, ((l, gt_call), gender)) => (l, gender) -> gt_call
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
      })
    .map{ case ((l,clinAttr), agg) => l -> (clinAttr, agg)}
    .mapPartitions(it => {
        it.toList.groupBy(_._1)
        .mapValues(_.map(_._2)).iterator
    }, true)
    q1_dict.cache
    q1_dict.count
    var end2 = System.currentTimeMillis() - start2

    if (get_skew){
      val p3 = q1_dict.mapPartitionsWithIndex{
            case (i,rows) => Iterator((label,"q1bc_shred_query",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p3
    }
    
    //unshred
    var start3 = System.currentTimeMillis()
    val q1 = unshred(q1_flat, q1_dict)
    q1.cache
    q1.count
    var end3 = System.currentTimeMillis()
    var end = end3 - start
    var end4 = end3 - start3
    result ++= Seq((label,"q1bc_shred",region,end1))
    result ++= Seq((label,"q1bc_shred_query",region,end2))
    result ++= Seq((label,"q1bc_unshred",region,end4))
    result ++= Seq((label,"q1bc_shred_total",region,end))
    if (get_skew){
      val p4 = q1.mapPartitionsWithIndex{
            case (i,rows) => Iterator((label,"q1bc_unshred",region,i,rows.size))
        }.collect.toSeq
      result2 ++= p4
    }
  }

  def writeResult(): Unit = {
    Utils.writeResult(spark_session, result.toList, result2.toList, "q1bc", get_skew)
  }
}

object Query1Broadcast{
  def apply(spark_session: SparkSession) = new Query1Broadcast(spark_session)
}

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

  def unshred(flat: RDD[(Long, (String, Int))], dict: RDD[(Long, (List[(String, Double)], List[(String, Double)]))]) = {
    dict.join(flat).map{
      case (vid, (odds, (contig, start))) => (contig, start, odds)
    }
  }

  def testFlat(region: Long, vs: RDD[VariantContext], clin: Dataset[Row]): Unit = {
    
    if (get_skew){
      val p1 = vs.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_initial,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p1)
    }
    var start = System.currentTimeMillis()
    val genotypes = Utils.flattenVariants(vs)

    val mdd = clin.select("id", "iscase").rdd.map(s => (s.getString(0), s.getDouble(1).toString))   
    val bmi = clin.select("id", "bmi").where("bmi is not null").rdd.map(s => (s.getString(0), s.getDouble(1))).map{
             case (s, b) => b match {
                case b if b < 25 => (s, "underweight")
                case _ => (s, "overweight")
             }
           }
    /***val mddodds = Calc.oddsratio(genotypes, mdd)
    val bmiodds = Calc.oddsratio(genotypes, bmi)
    val result = mddodds.join(bmiodds)
    result.count
    var end = System.currentTimeMillis() - start


    if (get_skew){
      val p2 = result.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_flat,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p2)
    }

    printer.println(label+",q1_flat,"+region+","+end)
    printer.flush
    printer2.flush**/
  }

  def testShred(region: Long, vs: RDD[VariantContext], clin: Dataset[Row]): Unit = {
    //shred
    var start = System.currentTimeMillis()
    val (v_flat, v_dict) = Utils.shred2(vs)
    v_dict.count
    var end1 = System.currentTimeMillis() - start
    if (get_skew){
      val p3 = v_dict.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_shred,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //construct query
    var start2 = System.currentTimeMillis()
    val g_flat = Utils.flattenShredVariants(v_dict)
    val mdd = clin.select("id", "iscase").rdd.map(s =>(s.getString(0), s.getDouble(1).toString))
    val bmi = clin.select("id", "bmi").where("bmi is not null").rdd.map(s => (s.getString(0), s.getDouble(1))).map{
             case (s, b) => b match {
                case b if b < 25 => (s, "1.0")
                case _ => (s, "0.0")
             }
           }    
    /**val mddodds = Calc.oddsratioShred(g_flat, mdd)
    val bmiodds = Calc.oddsratioShred(g_flat, bmi)
    val results = mddodds.join(bmiodds)
    results.count
    var end2 = System.currentTimeMillis() - start2

    if (get_skew){
      val p3 = results.mapPartitionsWithIndex{
            case (i,rows) => Iterator((i,rows.size))
        }.map(r => label+",q1_shred_query,"+region+","+r._1 +","+r._2).collect.toList.mkString("\n")
      printer2.println(p3)
    }
    
    //unshred
    var start3 = System.currentTimeMillis()
    val q1 = unshred(v_flat, results)
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
    printer2.flush**/
  }

  def close(): Unit = {
    printer.close()
    printer2.close()
  }
}

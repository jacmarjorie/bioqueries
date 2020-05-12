package cs.ox.ac.uk.shred.test.xmark

/**
  * Site: Bag(people: Bag(id: String, name: String, ...), 
  *    closed_auctions: Bag( seller: String, buyer: String, item: String, ...), 
  *    regions: Bag( africa: Bag( item: String, location: String, name: String, ...),  
  *      europe: Bag( item: String, location: String, name: String, ...), ...) ...)
  */

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.databricks.spark.xml._
import org.apache.spark.rdd.RDD
import scala.xml._
import org.apache.spark.sql.types.{StructType, StructField, StringType}

class XReader(sc: SparkContext, partitions: Int) extends XTypes{
  
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  /**
    * Input a directory of split files from XMark data generator
    * These files need to have the new lines remove, (cat file | tr "\n" " ")
    */
  def getSites(xpath: String) = {
    sc.wholeTextFiles(xpath).map{
      xml => XML.loadString(xml._2)
    }//.repartition(partitions)
  }

  def shred(a1: RDD[scala.xml.Elem]): (RDD[(Long, scala.xml.Elem)], RDD[(Long, scala.xml.NodeSeq)], RDD[(Long, scala.xml.NodeSeq)]) = { 
    // auction flat 
    val lbl = a1.zipWithUniqueId
    val aflat = lbl.map{ case (i, l) => (l, i) } // l0 -> {(people: l0, closed_auctions: l0)}

    // auction dict 
    val p1 = lbl.map{case (i, l) => (l, i \ "people")} // ( l0 -> auction.people, (null, null) )
    val c1 = lbl.map{case (i, l) => (l, i \ "closed_auctions")} // ( l0 -> auction.closed_auctions, (null, null, null) )
    (aflat, p1, c1)
  }

}

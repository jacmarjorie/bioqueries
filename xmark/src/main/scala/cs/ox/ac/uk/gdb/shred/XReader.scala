package cs.ox.ac.uk.shred.test.xmark

/**
  * Site: Bag(people: Bag(id: String, name: String, ...), 
  *    closed_auctions: Bag( seller: String, buyer: String, item: String, ...), 
  *    regions: Bag( africa: Bag( item: String, location: String, name: String, ...),  
  *      europe: Bag( item: String, location: String, name: String, ...), ...) ...)
  */

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.databricks.spark.xml._
import org.apache.spark.rdd.RDD
import scala.xml._
import org.apache.spark.sql.types.{StructType, StructField, StringType}

class XReader(sc: SparkContext, xfile: String, partitions: Int) extends XTypes{
  
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val peopleSchema = StructType(Array(
    StructField("_id", StringType, nullable=true),
    StructField("name", StringType, nullable=true)))
  val people = sqlContext.read
    .format("com.databricks.spark.xml")
    .option("rootTag", "people")
    .option("rowTag", "person")
    .schema(peopleSchema)
    .load(xfile)
    .rdd.map{
      r => (r.getString(0), r.getString(1))
    }

  val closed = sqlContext.read
    .format("com.databricks.spark.xml")
    .option("rootTag", "closed_auctions")
    .option("rowTag", "closed_auction")
    .load(xfile)
    .rdd.map{
      r => (r.getStruct(r.fieldIndex("buyer")).getString(1), 
        r.getStruct(r.fieldIndex("seller")).getString(1), 
        r.getStruct(r.fieldIndex("itemref")).getString(1))
    }

   val europe = sqlContext.read
    .format("com.databricks.spark.xml")
    .option("rootTag", "europe")
    .option("rowTag", "item")
    .load(xfile).rdd.map{
      r => (r.getString(r.fieldIndex("_id")), 
            r.getString(r.fieldIndex("location")), 
            r.getString(r.fieldIndex("name")))
    }

   val africa = sqlContext.read
    .format("com.databricks.spark.xml")
    .option("rootTag", "africa")
    .option("rowTag", "item")
    .load(xfile).rdd.map{
      r => (r.getString(r.fieldIndex("_id")), 
            r.getString(r.fieldIndex("location")), 
            r.getString(r.fieldIndex("name")))
    }

  val auctions: RDD[site] = sc.parallelize(List((people.collect.toList, 
                                                closed.collect.toList,
                                                List((africa.collect.toList, europe.collect.toList)))))

  def shred(a1: RDD[site]) = { 

    // auction flat 
    val lbl = a1.zipWithUniqueId
    val aflat = lbl.map{ case (i, l) => (l, i) } // l0 -> {(people: l0, closed_auctions: l0)}

    // auction dict 
    val p1 = lbl.map{case ((p, c, r), l) => (l, p)} // ( l0 -> auction.people, (null, null) )
    val c1 = lbl.map{case ((p, c, r), l) => (l, c)} // ( l0 -> auction.closed_auctions, (null, null, null) )
    // this requires further shredding 
    val r1 = lbl.map{case ((p, c, r), l) => (l, r)} // ( l0 -> auction.regions, (l1, l1) ) 
    (aflat, p1, c1, r1)
  }

}

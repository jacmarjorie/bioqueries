package cs.ox.ac.uk.shred.test.xmark

/**
  * Site: Bag(people: Bag(id: String, name: String, ...), 
  *    closed_auctions: Bag( seller: String, buyer: String, item: String, ...), 
  *    regions: Bag( africa: Bag( item: String, location: String, name: String, ...),  
  *      europe: Bag( item: String, location: String, name: String, ...), ...) ...)
  */

import scala.xml._
import org.apache.spark.rdd.RDD

object XReader extends XTypes{
  
  // n = number of top level records
  // p = number of partitions
  def read(xfile: String): site = {
    val x = XML.loadFile(xfile)
    val africa = (x \ "regions" \ "europe" \ "item").map{ i =>
      ((i \ "@id").text, (i \ "location").text, (i \ "name").text) 
    }.toList

    val europe = (x \ "regions" \ "europe" \ "item").map{ i =>
      ((i \ "@id").text, (i \ "location").text, (i \ "name").text) 
    }.toList

    val people = (x \ "people" \ "person").map{ p =>
      ((p \ "@id").text, (p \ "name").text)
    }.toList
  
    val closed_auctions = (x \ "closed_auctions" \ "closed_auction").map{ c =>
      ((c \ "seller" \ "@person").text, (c \ "buyer" \ "@person").text, (c \ "itemref" \ "@item").text)
    }.toList
  
    (people, closed_auctions, List((africa, europe)))
  }

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

import scala.xml._

/**
  * Site: Bag(people: Bag(id: String, name: String, ...), 
  *    closed_auctions: Bag( seller: String, buyer: String, item: String, ...), 
  *    regions: Bag( africa: Bag( item: String, location: String, name: String, ...),  
  *      europe: Bag( item: String, location: String, name: String, ...), ...) ...)
  */

import org.apache.spark.rdd.RDD

trait XTypes {
  type people = List[(String, String)]
  type closed = List[(String, String, String)]
  type region = List[(String, String, String)]
  type site = (people, closed, List[(region, region)])
}


object XReader extends XTypes{
  
  def read(xfile: String, val n: Int = 1, val p: Int = 48): RDD[site] = {
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
  
    sc.parallelize(List.fill(n)((people, closed_auctions, List((africa, europe)))), p)
  }

  def shred(auctions: RDD[site]) = { 

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

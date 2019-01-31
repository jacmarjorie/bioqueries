package cs.ox.ac.uk.shred.test.xmark

/**
  * Query 8 from XMark
  * let $auction := doc("auction.xml") return 
  * for $p in $auction/site/people/person 
  * let $a :=
  *   for $t in $auction/site/closed_auctions/closed_auction where $t/buyer/@person = $p/@id
  *   return $t
  * return <item person="{$p/name/text()}">{count($a)}</item>
  *
  */

import java.io._
import org.apache.spark.rdd.RDD

object XMark8 extends XTypes {

  var outfile = "/mnt/app_hdd/scratch/xmark/xmark8.csv"
  @transient val printer = new PrintWriter(new FileOutputStream(new File(outfile), true /* append = true */))

  /**
    * Flattened query
    *
    * for site1 in auction 
    * for person in site1.people
    *    sng ( ( person.name, Mult( person.id, for site2 in auction 
    *                            for closed in site2.closed_auctions
    *                                if closed.buyer = person.id
    *                                then sng( person.id ) ) ) )
    */
  def flat(a1: RDD[site], test: String) = {
    
    var start = System.currentTimeMillis()
    /// flatten data
    val pflat = a1.flatMap{ case (person, closed, regions) => person.map{
      case (pid, pname) => (pid, pname)
    }}

    val cflat = a1.flatMap{ case (person, closed, regions) => closed.map{
      case (buyer, seller, item) => (buyer, (seller, item))
    }}

    val flatq = pflat.join(cflat).map{
      case (pid, (pname, (seller, item))) => ((pid,pname), 1)
    }.reduceByKey{
      case ((cnt1),(cnt2)) => (cnt1 + cnt2)
    }.map{
      case ((pid, pname), cnt) => (pname, cnt)
    }
    flatq.count
    var end = System.currentTimeMillis() - start
    
    printer.println("flat_total"+test+","+end)
    printer.flush

  }

  /**
    * Shred query no optimizations
    *
    * NewLabel() -> for site1^flat in project1(auction^dict)(auction^flat)
    *            for person in project1(site1.people^dict)(site1.people^flat)
    *                sng (person.name, Mult(person.id, for site2^flat in project1(auction^dict)(auction^flat) 
    *                                                    for closed^flat in project1(site2.closed^dict)(site2.closed^flat) 
    *                                                        if closed^flat.buyer = person^flat.id then sng(person^flat.id)))
    *
    * DU is null
    */
  def shred(xr: XReader, a1: RDD[site], test: String) = {

    var start = System.currentTimeMillis()
    val (aflat, p1, c1, r1) = xr.shred(a1)
    var shredt = System.currentTimeMillis() - start

    var start1 = System.currentTimeMillis()
    val p1_flat = aflat.join(p1).flatMap{
      case (l, (a, p)) => p.map{
        case (pid, pname) => ((pid,l), pname)
      }
    }
    
    val c1_flat = aflat.join(c1).flatMap{
        case (l, (a, c)) => c.map{ 
          case (buyer, seller, item) => ((buyer,l), (seller, item))
        }
      }

    val b2 = p1_flat.join(c1_flat).map{
          case ((pid, l), (pname, (seller, item))) => ((pid, l), 1)
        }.reduceByKey{
          case ((cnt1), (cnt2)) => (cnt1 + cnt2)
        }
    b2.count
    var shredq = System.currentTimeMillis() - start1    

    printer.println("shred_shred"+test+","+shredt)
    printer.println("shred_shredq"+test+","+shredq)
    printer.flush
  }

  /**
    * Shred query optimizations
    *
    * NewLabel() -> for site1^flat in project1(auction^dict)(auction^flat)
    *            for person in project1(site1.people^dict)(site1.people^flat)
    *                sng (person.name, Mult(person.id, for site2^flat in project1(auction^dict)(auction^flat) 
    *                                                    for closed^flat in project1(site2.closed^dict)(site2.closed^flat) 
    *                                                        if closed^flat.buyer = person^flat.id then sng(person^flat.id)))
    *
    * DU is null
    */
  def shredOpt(xr: XReader, a1: RDD[site], test: String) = {

    var start = System.currentTimeMillis()
    val (aflat, p1, c1, r1) = xr.shred(a1)
    var shredt = System.currentTimeMillis() - start

    var start1 = System.currentTimeMillis()
    val p1_flat = p1.flatMap{
      case (l, p) => p.map{
        case (pid, pname) => ((pid,l), pname)
      }
    }

    val c1_flat = c1.flatMap{
        case (l, c) => c.map{ 
          case (buyer, seller, item) => ((buyer,l), (seller, item))
        }
      }

    val b2 = p1_flat.join(c1_flat).map{
          case ((pid, l), (pname, (seller, item))) => ((pid, pname, l), 1)
        }.reduceByKey{
          case ((cnt1), (cnt2)) => (cnt1 + cnt2)
        }.map{
          case ((pid, pname, l), cnt) => (pname, cnt)
        }
    b2.count
    var shredqt = System.currentTimeMillis() - start1    
   
    printer.println("shredOpt_shred"+test+","+shredt)
    printer.println("shredOpt_shredq"+test+","+shredqt)
    printer.flush
  }


}

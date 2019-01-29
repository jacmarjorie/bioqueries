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

  // map nested operations over top level records
  // as defined by a user function
  def uf(a1: RDD[site]) = {
    // todo
  }

  // flatten the data and execute the query
  def flat(a1: RDD[site], test: String) = {
    
    var start = System.currentTimeMillis()
    /// flatten data
    val pflat = a1.flatMap{ case (person, closed, regions) => person.map{
      case (pid, pname) => (pid, pname)
    }}

    val cflat = a1.flatMap{ case (person, closed, regions) => closed.map{
      case (buyer, seller, item) => (buyer, (seller, item))
    }}
    //var end1 = System.currentTimeMillis() - start

    //var start1 = System.currentTimeMillis()
    val flatq = pflat.join(cflat).map{
      case (pid, (pname, (seller, item))) => ((pid,pname), 1)
    }.reduceByKey{
      case ((cnt1),(cnt2)) => (cnt1 + cnt2)
    }.map{
      case ((pid, pname), cnt) => (pname, cnt)
    }
    //var end2 = System.currentTimeMillis() - start1
    flatq.count
    var end = System.currentTimeMillis() - start
    
    // write results
    //printer.println("flat_flatten"+test+","+end1)
    //printer.println("flat_join"+test+","+end2)
    printer.println("flat_total"+test+","+end)
    printer.flush

  }

  // shred query - no optimizations
  def shred(a1: RDD[site], test: String) = {

    var start = System.currentTimeMillis()
    val (aflat, p1, c1, r1) = XReader.shred(a1)
    var shredt = System.currentTimeMillis() - start

    var start1 = System.currentTimeMillis()
    val b2 = aflat.join(p1).flatMap{
      case (l, (a, p)) => p.map{
        case (pid, pname) => ((pid,l), pname)
      }
    }
    b2.count
    var shredb2 = System.currentTimeMillis() - start1
    
    var start2 = System.currentTimeMillis()
    val p1_flat = aflat.join(p1).flatMap{
      case (l, (a, p)) => p.map{
        case (pid, pname) => ((pid, l), pname)
      }
    }

    val c1_flat = aflat.join(c1).flatMap{
        case (l, (a, c)) => c.map{ 
          case (buyer, seller, item) => ((buyer,l), (seller, item))
        }
      }

    val du = p1_flat.join(c1_flat).map{
          case ((pid, l), (pname, (seller, item))) => ((pid, l), 1)
        }.reduceByKey{
          case ((cnt1), (cnt2)) => (cnt1 + cnt2)
        }
    du.count
    var shredq = System.currentTimeMillis() - start2    

    var start3 = System.currentTimeMillis()
    // unshred
    val result = du.join(b2).map{
      case ((pid,l), (cnt, pname)) => (pname, cnt)
    }
    result.count
    var unshred = System.currentTimeMillis() - start3
    printer.println("shred_shred"+test+","+shredt)
    printer.println("shred_flatq"+test+","+shredb2)
    printer.println("shred_shredq"+test+","+shredq)
    printer.println("shred_shredq_total"+test+","+(shredb2+shredq))
    printer.println("shred_unshred"+test+","+unshred)
    printer.flush
  }

  // shred with optimizations
  // top level join not needed 
  def shredOpt(a1: RDD[site], test: String) = {

    var start = System.currentTimeMillis()
    val (aflat, p1, c1, r1) = XReader.shred(a1)
    var shredt = System.currentTimeMillis() - start

    var start1 = System.currentTimeMillis()
    val b2 = p1.flatMap{
      case (l, p) => p.map{
        case (pid, pname) => ((pid,l), pname)
      }
    }
    b2.count
    var shredb2 = System.currentTimeMillis() - start
    
    var start2 = System.currentTimeMillis()
    val p1_flat = b2

    val c1_flat = c1.flatMap{
        case (l, c) => c.map{ 
          case (buyer, seller, item) => ((buyer,l), (seller, item))
        }
      }

    val du = p1_flat.join(c1_flat).map{
          case ((pid, l), (pname, (seller, item))) => ((pid, l), 1)
        }.reduceByKey{
          case ((cnt1), (cnt2)) => (cnt1 + cnt2)
        }
    du.count
    var shredq = System.currentTimeMillis() - start2    
   
    // unshred
    var start3 = System.currentTimeMillis() 
    val shredq = du.join(b2).map{
      case ((pid,l), (cnt, pname)) => (pname, cnt)
    }
    shredq.count
    var unshred = System.currentTimeMillis() - start3
    printer.println("shredOpt_shred"+test+","+shredt)
    printer.println("shredOpt_flatq"+test+","+shredb2)
    printer.println("shredOpt_shredq"+test+","+shredq)
    printer.println("shredOpt_shredq_total"+test+","+(shredb2+shredq))
    printer.println("shredOpt_unshred"+test+","+unshred)
    printer.flush
  }


}

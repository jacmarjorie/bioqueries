package cs.ox.ac.uk.shred.test.xmark

trait XTypes {
  type people = List[(String, String)]
  type closed = List[(String, String, String)]
  type region = List[(String, String, String)]
  type site = (people, closed, List[(region, region)])
}


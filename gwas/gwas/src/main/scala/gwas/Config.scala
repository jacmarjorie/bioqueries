package gwas

/** Config that reads data.flat. 
  * Currently reads fields that are specific to tpch
  */
object Config {

  val prop = new java.util.Properties
  val fsin = new java.io.FileInputStream("db-properties.flat")
  prop.load(fsin)

  val hostfile = prop.getProperty("hostfile")
  val loader = prop.getProperty("loader")
  val workspace = prop.getProperty("workspace")
  val array = prop.getProperty("array")
  val clinical = prop.getProperty("clinical")
  val dbprops = prop.getProperty("dbprops")

}

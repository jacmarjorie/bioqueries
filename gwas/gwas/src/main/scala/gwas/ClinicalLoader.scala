package gwas

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}

case class ThousandGenomes(sample: String, family_id: String, population: String, gender: String)

class ClinicalLoader(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val schema = StructType(Array(
                    StructField("sample", StringType), 
                    StructField("family_id", StringType),
                    StructField("population", StringType),
                    StructField("gender", StringType)))

  val tgenomes = spark.read.schema(schema)
    .option("header", true)
    .option("delimiter", ",")
    .csv("/Users/jac/bioqueries/data/1000g.csv")
    .as[ThousandGenomes]

}


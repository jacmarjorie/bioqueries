package gwas

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, DoubleType, StructField, StructType}

case class ClinicalSub(id: String, iscase: Option[Int])
case class Clinical(
  index: Option[Int], srr_id: String, id: String, city: String, province: String, hospital_code: String, 
  iscase: Option[Int], dep_e27: Option[Int], dep_e29: Option[Int], md_a1: Option[Int], md_a2: Option[Int], 
  md_a3: Option[Int], md_a4: Option[Int],
  md_a5: Option[Int], md_a6: Option[Int], md_a7: Option[Int], md_a8: Option[Int], md_a9: Option[Int], 
  me: Option[Int], panic: Option[Int], 
  gad: Option[Int], dysthymia: Option[Int], postnatal_d: Option[Int], neuroticism: Option[Int], 
  csa: Option[Int], sle: Option[Int], 
  cold_m: Option[Int], auth_m: Option[Int], prot_m: Option[Int], cold_f: Option[Int], auth_f: Option[Int], prot_f: Option[Int], 
  pms: Option[Int], agora_diag: Option[Int], social_diag: Option[Int], animal_diag: Option[Int], situational_diag: Option[Int], 
  blood_diag: Option[Int], phobia: Option[Int], suicidal_thought: Option[Int], suicidal_plan: Option[Int], 
  suicidal_attempt: Option[Int], dob_y: Option[Int], dob_m: Option[Int], dob_d: Option[Int], age: Option[Int], education: Option[Int],
  occupation: Option[Int], social_class: Option[Int], martial_status: Option[Int], fh_count: Option[Int], 
  height_clean: Option[Int], weight_clean: Option[Int], bmi: Option[Double])

class ClinicalLoader(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val schemaSub = StructType(Array(
                    StructField("id", StringType), 
                    StructField("iscase", IntegerType, nullable=true)))

  val schema = StructType(Array(
                  StructField("index", IntegerType, nullable=true), 
                  StructField("srr_id", StringType),
                  StructField("id", StringType),
                  StructField("city", StringType), 
                  StructField("province", StringType), 
                  StructField("hospital_code", StringType),
                  StructField("iscase", IntegerType, nullable=true), 
                  StructField("dep_e27", IntegerType, nullable=true),
                  StructField("dep_e29", IntegerType, nullable=true),
                  StructField("md_a1", IntegerType, nullable=true),
                  StructField("md_a2", IntegerType, nullable=true),
                  StructField("md_a3", IntegerType, nullable=true),
                  StructField("md_a4", IntegerType, nullable=true),
                  StructField("md_a5", IntegerType, nullable=true),
                  StructField("md_a6", IntegerType, nullable=true),
                  StructField("md_a7", IntegerType, nullable=true),
                  StructField("md_a8", IntegerType, nullable=true),
                  StructField("md_a9", IntegerType, nullable=true),
                  StructField("me", IntegerType, nullable=true),
                  StructField("panic", IntegerType, nullable=true),
                  StructField("gad", IntegerType, nullable=true),
                  StructField("dysthymia", IntegerType, nullable=true),
                  StructField("postnatal_d", IntegerType, nullable=true),
                  StructField("neuroticism", IntegerType, nullable=true),
                  StructField("csa", IntegerType, nullable=true),
                  StructField("sle", IntegerType, nullable=true),
                  StructField("cold_m", IntegerType, nullable=true),
                  StructField("auth_m", IntegerType, nullable=true),
                  StructField("prot_m", IntegerType, nullable=true),
                  StructField("cold_f", IntegerType, nullable=true),
                  StructField("auth_f", IntegerType, nullable=true),
                  StructField("prot_f", IntegerType, nullable=true),
                  StructField("pms", IntegerType, nullable=true),
                  StructField("agora_diag", IntegerType, nullable=true),
                  StructField("social_diag", IntegerType, nullable=true),
                  StructField("animal_diag", IntegerType, nullable=true),
                  StructField("situational_diag", IntegerType, nullable=true),                  
                  StructField("blood_diag", IntegerType, nullable=true),
                  StructField("phobia", IntegerType, nullable=true),
                  StructField("suicidal_thought", IntegerType, nullable=true),
                  StructField("suicidal_plan", IntegerType, nullable=true),
                  StructField("suicidal_attempt", IntegerType, nullable=true),
                  StructField("dob_y", IntegerType, nullable=true),                  
                  StructField("dob_m", IntegerType, nullable=true),
                  StructField("dob_d", IntegerType, nullable=true),
                  StructField("age", IntegerType, nullable=true),
                  StructField("education", IntegerType, nullable=true),
                  StructField("occupation", IntegerType, nullable=true),                  
                  StructField("social_class", IntegerType, nullable=true),
                  StructField("martial_status", IntegerType, nullable=true),
                  StructField("fh_count", IntegerType, nullable=true),
                  StructField("height_clean", IntegerType, nullable=true),
                  StructField("weight_clean", IntegerType, nullable=true),
                  StructField("bmi", DoubleType, nullable=true)))

  val clinical = spark.read.schema(schema)
    .option("header", true)
    .option("delimiter", ",")
    .csv("/Users/jac/converge/converge_sub.csv")
    .as[ClinicalSub]

}


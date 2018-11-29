package cs.ox.ac.uk.shred.test.converge

import org.apache.spark.rdd.RDD
import java.net.{URL, URLConnection, HttpURLConnection}
import javax.net.ssl._
import java.security.cert.X509Certificate
import java.io._
import scala.util.parsing.json._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class AnnotationHelper(spark_session: SparkSession, server: String, ext: String){
    
    // val server = "https://rest.ensembl.org"
    // val ext = "/vep/human/id"
            
    import spark_session.implicits._
    val url = new URL(server + ext)
    
    def makeRequest(snps: RDD[((String, Int), Int)]) = {
      val requestGroups = snps.map(s => "\"rs"+s._2+"\"").collect.toList.grouped(200)
      requestGroups.map(grp =>  
                  getAnnotations("{ \"ids\": ["+grp.mkString(",")+" ] }").rdd).reduce(_ union _)
                      //.map( annot => 
                      //Integer.parseInt(annot.getString(4).replace("rs", "")) -> annot)).reduce(_ union _)
    }
    
    def getAnnotations(postBody: String): DataFrame = {
        val connection = url.openConnection()
        val httpConnection = connection.asInstanceOf[HttpURLConnection]
        
        val sslContext = SSLContext.getInstance("SSL")
        sslContext.init(null, Array(TrustAll), new java.security.SecureRandom())
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
        HttpsURLConnection.setDefaultHostnameVerifier(VerifiesAllHostNames)

        httpConnection.setRequestMethod("POST")
        httpConnection.setRequestProperty("Content-Type", "application/json")
        httpConnection.setRequestProperty("Accept", "application/json")
        httpConnection.setRequestProperty("Content-Length", Integer.toString(postBody.getBytes().length))
        httpConnection.setUseCaches(false)
        httpConnection.setDoInput(true)
        httpConnection.setDoOutput(true)
        
        val wr = new DataOutputStream(httpConnection.getOutputStream())
        wr.writeBytes(postBody)
        wr.flush()
        wr.close()
        
        
        val response = connection.getInputStream()
        val responseCode = httpConnection.getResponseCode()
        
        if(responseCode != 200) {
          throw new RuntimeException("Response code was not 200. Detected response was "+responseCode)
        }
        
        val responseText = new java.util.Scanner(connection.getInputStream, "UTF-8").useDelimiter("\\A").next()

        val responseRDD = spark_session.sparkContext.makeRDD(responseText :: Nil)
        spark_session.read.json(responseRDD)
    }
}

object AnnotationHelper{
  def apply(spark_session: SparkSession, server: String, ext: String) = new AnnotationHelper(spark_session, server, ext)
}

// Bypasses both client and server validation.
object TrustAll extends X509TrustManager {
  val getAcceptedIssuers = null

  def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String) = {}

  def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String) = {}
}

// Verifies all host names by simply returning true.
object VerifiesAllHostNames extends HostnameVerifier {
  def verify(s: String, sslSession: SSLSession) = true
}

package main.scala.com.equinix.bdp.poc

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.text.SimpleDateFormat

class JoinSample {

  def main(args: Array[String]) {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    case class Register (d: java.util.Date, uuid: String, cust_id: String, lat: Float, lng: Float)
    
  }
}
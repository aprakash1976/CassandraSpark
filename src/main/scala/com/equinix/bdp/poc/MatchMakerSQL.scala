package main.scala.com.equinix.bdp.poc

import java.nio.ByteBuffer
import java.util.SortedMap
import scala.collection.JavaConversions._
import org.apache.cassandra.db.Column
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat
import org.apache.cassandra.thrift._
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.hadoop.mapreduce.Job
//import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MatchMakerSQL {

  def main(args: Array[String]) {
	val sparkConf = new SparkConf().setAppName("MATCHMAKER")
    val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)
	
	case class SCRawData(region: String, accountName: String)
	
	val job = new Job()
    
    val host: String = "sv2lxdprep02.corp.equinix.com"
    val port: String = "9160"
    
    ConfigHelper.setInputInitialAddress(job.getConfiguration(), host)
    ConfigHelper.setInputRpcPort(job.getConfiguration(), port)
    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), host)
    ConfigHelper.setOutputRpcPort(job.getConfiguration(), port)
    ConfigHelper.setInputColumnFamily(job.getConfiguration(), "MATCHMAKER", "MATCH_MAKER_SC_RAW_DATA")
    ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "MATCHMAKER", "MATCH_MAKER_SC_RAW_DATA")

    val predicate = new SlicePredicate()
    val sliceRange = new SliceRange()
    sliceRange.setStart(Array.empty[Byte])
    sliceRange.setFinish(Array.empty[Byte])
    predicate.setSlice_range(sliceRange)
    ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate)
    
    ConfigHelper.setInputPartitioner(job.getConfiguration(), "RandomPartitioner")
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "RandomPartitioner")
    
    val matchmakerRDD = sc.newAPIHadoopRDD(job.getConfiguration(), classOf[ColumnFamilyInputFormat], classOf[ByteBuffer], classOf[SortedMap[ByteBuffer, Column]])

    val matchmaker = matchmakerRDD.filter { case (k,v) => ByteBufferUtil.string(v.get(ByteBufferUtil.bytes("PARTY_REGION")).value())=="US" }
	
	val paraRdd = matchmakerRDD.map {
      case (key, value) => {
        ByteBufferUtil.string(value.get(ByteBufferUtil.bytes("PARTY_REGION")).value())
      }
    }
	//matchmaker.saveAsTextFile("file:///home/gdev/rawdata")
    
	val count = matchmaker.count
	
	println("number of us acount records : " + count)
  }
}
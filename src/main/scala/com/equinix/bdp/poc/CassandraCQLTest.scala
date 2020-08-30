package main.scala.com.equinix.bdp.poc

import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
Need to create following keyspace and column family in cassandra before running this example
Start CQL shell using ./bin/cqlsh and execute following commands
CREATE KEYSPACE retail WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use retail;
CREATE TABLE salecount (prod_id text, sale_count int, PRIMARY KEY (prod_id));
CREATE TABLE ordercf (user_id text,
time timestamp,
prod_id text,
quantity int,
PRIMARY KEY (user_id, time));
INSERT INTO ordercf (user_id,
time,
prod_id,
quantity) VALUES ('bob', 1385983646000, 'iphone', 1);
INSERT INTO ordercf (user_id,
time,
prod_id,
quantity) VALUES ('tom', 1385983647000, 'samsung', 4);
INSERT INTO ordercf (user_id,
time,
prod_id,
quantity) VALUES ('dora', 1385983648000, 'nokia', 2);
INSERT INTO ordercf (user_id,
time,
prod_id,
quantity) VALUES ('charlie', 1385983649000, 'iphone', 2);
*/
 
/**
* This example demonstrates how to read and write to cassandra column family created using CQL3
* using Spark.
* Parameters : <spark_master> <cassandra_node> <cassandra_port>
* Usage: ./bin/run-example org.apache.spark.examples.CassandraCQLTest local[2] localhost 9160
*
*/
object CassandraCQLTest {

  def main(args: Array[String]) {
    /*val sc = new SparkContext(args(0),
               "CQLTestApp",
               System.getenv("SPARK_HOME"),
               SparkContext.jarOfClass(this.getClass))*/
    val sparkConf = new SparkConf().setAppName("MATCHMAKER")
    val sc = new SparkContext(sparkConf)
    sc.addJar("http://repo1.maven.org/maven2/org/apache/cassandra/cassandra-all/2.0.6/cassandra-all-2.0.6.jar")
    sc.addJar("http://repo1.maven.org/maven2/org/apache/cassandra/cassandra-thrift/2.0.6/cassandra-thrift-2.0.6.jar")
    sc.addJar("http://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.9.1/libthrift-0.9.1.jar")
    val cHost: String = "sv2lxdpdseqa01.corp.equinix.com"
    val cPort: String = "9160"
    val KeySpace = "precog"
    val InputColumnFamily = "user_info"
    val OutputColumnFamily = "user_info"

    val job = new Job()
    job.setInputFormatClass(classOf[CqlPagingInputFormat])
    ConfigHelper.setInputInitialAddress(job.getConfiguration(), cHost)
    ConfigHelper.setInputRpcPort(job.getConfiguration(), cPort)
    ConfigHelper.setInputColumnFamily(job.getConfiguration(), KeySpace, InputColumnFamily)
    ConfigHelper.setInputPartitioner(job.getConfiguration(), "RandomPartitioner")
    CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3")

    /** CqlConfigHelper.setInputWhereClauses(job.getConfiguration(), "user_id='bob'") */

    /** An UPDATE writes one or more columns to a record in a Cassandra column family */
    val query = "UPDATE " + KeySpace + "." + OutputColumnFamily + " SET aggr_count = ? "
    CqlConfigHelper.setOutputCql(job.getConfiguration(), query)

    job.setOutputFormatClass(classOf[CqlOutputFormat])
    ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KeySpace, OutputColumnFamily)
    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), cHost)
    ConfigHelper.setOutputRpcPort(job.getConfiguration(), cPort)
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "RandomPartitioner")

    val casRdd = sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[CqlPagingInputFormat],
      classOf[java.util.Map[String,ByteBuffer]],
      classOf[java.util.Map[String,ByteBuffer]])

    val productSaleRDD = casRdd.map {
      case (key, value) => {
        (ByteBufferUtil.string(value.get("fname")), ByteBufferUtil.toInt(ByteBuffer.wrap("1".getBytes())))
        //ByteBufferUtil.string(value.get("fname"))
      }
    }
    val aggregatedRDD = productSaleRDD.reduceByKey(_ + _)
    //val counts = productSaleRDD.flatMap(p => p.split(" ")).map(fname => (fname, 1)).reduceByKey(_ + _)
    /*counts.collect().foreach {
      case (word, count) => println(fname + ":" + count)
    }*/
    /*aggregatedRDD.collect().foreach {
      case (fname, aggr_count) => println(fname + ":" + aggr_count)
    }*/
    val counters = aggregatedRDD.count()

    /*val casoutputCF = aggregatedRDD.map {
      case (status, aggr_count) => {
        val outColFamKey = Map("status" -> ByteBufferUtil.bytes(status))
        val outKey: java.util.Map[String, ByteBuffer] = outColFamKey
        var outColFamVal = new ListBuffer[ByteBuffer]
        outColFamVal += ByteBufferUtil.bytes(aggr_count)
        val outVal: java.util.List[ByteBuffer] = outColFamVal
       (outKey, outVal)
      }
    }

    casoutputCF.saveAsNewAPIHadoopFile(
        KeySpace,
        classOf[java.util.Map[String, ByteBuffer]],
        classOf[java.util.List[ByteBuffer]],
        classOf[CqlOutputFormat],
        job.getConfiguration()
      )*/
  }
}
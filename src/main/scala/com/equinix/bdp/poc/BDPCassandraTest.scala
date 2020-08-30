package main.scala.com.equinix.bdp.poc


import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat
import org.apache.cassandra.thrift._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.nio.ByteBuffer
import java.util.SortedMap
import org.apache.cassandra.db.Column
import org.apache.cassandra.utils.ByteBufferUtil
import scala.collection.JavaConversions._
/*import org.apache.cassandra.db.IColumn

*/
/*
 * This example demonstrates using Spark with Cassandra with the New Hadoop API and Cassandra
 * support for Hadoop.
 *
 * To run this example, run this file with the following command params -
 * <spark_master> <cassandra_node> <cassandra_port>
 *
 * So if you want to run this on localhost this will be,
 * local[3] localhost 9160
 *
 * The example makes some assumptions:
 * 1. You have already created a keyspace called casDemo and it has a column family named Words
 * 2. There are column family has a column named "para" which has test content.
 *
 * You can create the content by running the following script at the bottom of this file with
 * cassandra-cli.
 *
 */
object BDPCassandraTest {

  def main(args: Array[String]) { 
	  // Get a SparkContext
    val sc = new SparkContext(args(0), "casDemo")

    // Build the job configuration with ConfigHelper provided by Cassandra
    val job = new Job()
    job.setInputFormatClass(classOf[ColumnFamilyInputFormat])

    val host: String = "sv2lxdpdseqa01.corp.equinix.com"
    val port: String = "9160"

    ConfigHelper.setInputInitialAddress(job.getConfiguration(), host)
    ConfigHelper.setInputRpcPort(job.getConfiguration(), port)
    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), host)
    ConfigHelper.setOutputRpcPort(job.getConfiguration(), port)
    ConfigHelper.setInputColumnFamily(job.getConfiguration(), "precog", "user_info")
    ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "precog", "user_info")

    /*val predicate = new SlicePredicate()
    val sliceRange = new SliceRange()
    sliceRange.setStart(Array.empty[Byte])
    sliceRange.setFinish(Array.empty[Byte])
    predicate.setSlice_range(sliceRange)
    ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate)*/

    ConfigHelper.setInputPartitioner(job.getConfiguration(), "RandomPartitioner")
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "RandomPartitioner")

    // Make a new Hadoop RDD
    /*val casRdd = sc.newAPIHadoopRDD(
      job.getConfiguration(),
      classOf[ColumnFamilyInputFormat],
      classOf[ByteBuffer],
      classOf[SortedMap[ByteBuffer, IColumn]])*/
    val casRdd = sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[CqlPagingInputFormat],
      classOf[java.util.Map[String,ByteBuffer]],
      classOf[java.util.Map[String,ByteBuffer]])

    // Let us first get all the paragraphs from the retrieved rows
    val paraRdd = casRdd.map {
      case (key, value) => {
        //ByteBufferUtil.string(value.get(ByteBufferUtil.bytes("fname")))
        (ByteBufferUtil.string(value.get("fname")), ByteBufferUtil.toInt(ByteBuffer.wrap("1".getBytes())))
      }
    }

    // Lets get the word count in paras
    //val counts = paraRdd.flatMap(p => p.split(" ")).map(fname => (fname, 1)).reduceByKey(_ + _)
    val counts = paraRdd.reduceByKey(_ + _)
    
    counts.collect().foreach {
      case (fname, count) => println(fname + ":" + count)
    }

    counts.map {
      case (fname, count) => {
        val colWord = new org.apache.cassandra.thrift.Column()
        colWord.setName(ByteBufferUtil.bytes("fname"))
        colWord.setValue(ByteBufferUtil.bytes(fname))
        colWord.setTimestamp(System.currentTimeMillis)

        val colCount = new org.apache.cassandra.thrift.Column()
        colCount.setName(ByteBufferUtil.bytes("count"))
        colCount.setValue(ByteBufferUtil.bytes(count.toLong))
        colCount.setTimestamp(System.currentTimeMillis)

        val outputkey = ByteBufferUtil.bytes(fname + "-COUNT-" + System.currentTimeMillis)

        val mutations: java.util.List[Mutation] = new Mutation() :: new Mutation() :: Nil
        mutations.get(0).setColumn_or_supercolumn(new ColumnOrSuperColumn())
        mutations.get(0).column_or_supercolumn.setColumn(colWord)
        mutations.get(1).setColumn_or_supercolumn(new ColumnOrSuperColumn())
        mutations.get(1).column_or_supercolumn.setColumn(colCount)
        (outputkey, mutations)
      }
    }.saveAsNewAPIHadoopFile("casDemo", classOf[ByteBuffer], classOf[List[Mutation]],
      classOf[ColumnFamilyOutputFormat], job.getConfiguration)
  }
}
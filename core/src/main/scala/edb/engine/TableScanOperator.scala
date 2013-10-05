package edb.engine

import edb.parser._
import edb.catalog._
import edb.enviroment._
import spark.SparkContext
import SparkContext._
import spark.{RDD}
import spark.{Logging}
import scala.reflect.BeanProperty
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io._
import org.apache.hadoop.fs._
import org.apache.hadoop.util._
import org.apache.hadoop.mapred._
import org.apache.hadoop.conf._

class TableScanOperator (tableName: String) extends 
UnaryOperator[SequenceRecord]{

 private var hdfsPath: String = null

  //setup operator meta data
  override def initializeOnMaster() {
    //for tableScan operator, in and out are the same schema
    setInSch(Catalog.getTableSchema(tableName))
    setOutSch(Catalog.getTableSchema(tableName))
    logInfo("tsc: name " + tableName) 
    hdfsPath = Catalog.getTableLocation(tableName)
    logInfo("tsc: path " + hdfsPath) 
    assert(hdfsPath.size >0)
    assert(inSch!= null)
  }

  override def execute(): RDD[_] = {
    //talbe scan operator is the leaf operator
    assert(parentOperators.size == 0)
    super.execute()
  }

  override def processPartition(index: Int, 
    iter: Iterator[_]): Iterator[_] = iter


  /** load hadoopRDD from HDFS file 
    * Called on master by UnaryOperator::execute() 
    */
  override def preprocessRdd(rdd: RDD[_]): RDD[SequenceRecord] = {
    //hard code inputformat class now
    val ifc = classOf
    [SequenceFileInputFormat[IntWritable,SequenceRecord]]
    logInfo("Table input: %s".format(hdfsPath))
    assert(hdfsPath.size >0)
    createHadoopRdd(hdfsPath, ifc)
  }

  private def createHadoopRdd(path: String, 
    ifc : Class[_ <: InputFormat[IntWritable, SequenceRecord]]): RDD[SequenceRecord] = {

    val jconf =  new JobConf(new Configuration());

    //setup hdfs path for jconf
    FileInputFormat.addInputPath(jconf, new Path(path));

    val buffersize = System.getProperty("spark.buffer.size", "65536")
    jconf.set("io.file.buffer.size", buffersize)

    // choose the minimum number of splits. 
    //if mapred.map.tasks is set, use that unless
    // it is smaller than what spark suggests.
    val minsplits = math.max(jconf.getInt("mapred.map.tasks", 1), 1) 

    val hRdd = EdbEnv.sc.hadoopRDD(jconf,
      ifc, classOf[IntWritable], 
      classOf[SequenceRecord] ,minsplits) 
    hRdd.map(x=>x._2.copy())
  }

  override def toString(): String = 
     "TABLESCAN--table name: " + tableName
      
}



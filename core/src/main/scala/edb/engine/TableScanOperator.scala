package edb.engine

import edb.parser._
import edb.catalog._
import edb.enviroment._

import scala.reflect.BeanProperty
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD}
import org.apache.spark.{Logging}

/*
 * Copyright (C) 2013 The Regents of Mingxi Wu
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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



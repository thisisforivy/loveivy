package edb.engine

import edb.catalog._

import spark.SparkContext
import SparkContext._
import spark.{RDD}

import scala.reflect.BeanProperty
import org.apache.hadoop.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.mapred._
import org.apache.hadoop.fs._
import org.apache.hadoop.util._

object FileConstant { final val INPUT_SPLIT= 1 }

sealed abstract class FileType

case class SequenceDBFile extends FileType
case class TextDBFile extends FileType

class  DBFile {

  //dir to hold the file.e.g. /edb/tablespace/tableName/part-*
  @BeanProperty var fileLocation: String = _
  @BeanProperty var sch: Schema = _
  @BeanProperty var fileType: FileType = _
  @BeanProperty var rdd: RDD[(EdbIntWritable,SequenceRecord)]  = _
  @BeanProperty var sc: SparkContext = _

  //fs setup
  val conf = new Configuration()
  conf.addResource(new Path("/server/hadoop/conf/core-site.xml"))
  val fs = FileSystem.get(conf)

  /**
    * Initialize basic file meta data before we bring it in 
    * memory
    *
    * @param fileLoc the string of the hdfs path
    * @param sch the schema of the file
    * @param fType the file type, default is null.
    * @param sc spark context used for create file handler 
    */
  def init(fileLoc: String, 
    sch:Schema, 
    fType: FileType = null,
    sc: SparkContext = null) {

    setFileLocation(fileLoc)
    setSch(sch)
    setFileType(fType)
    setSc(sc)
  }

  /**
    * Link the HDFS file dir to memory RDD representation
    * @return Trun on succesful load, False otherwise
    *
    */
  def link (cache: Boolean){

    //check existence before cache a base table
    if (cache){
      assert (fs.exists(new Path(fileLocation)))
    }

    val fileLoc = new Path(fileLocation + "/part-*")

    //link the file to RDD
    val jconf =  new JobConf(new Configuration())
    FileInputFormat.addInputPath(jconf, fileLoc)

    val initHandle = sc.hadoopRDD(jconf,
      classOf[SequenceFileInputFormat[EdbIntWritable,SequenceRecord]], 
      classOf[EdbIntWritable], 
      classOf[SequenceRecord] ,
      FileConstant.INPUT_SPLIT)

    //make distinct copy of each (k,v)
    rdd = initHandle.map(x=>(x._1.copy(),x._2.copy()))

    if (cache){
      rdd.cache()
    }
  }

  /**
    * Persist the rdd to HDFS
    */
  def save (path: String){

  //  if (path != null) {
      //remove the old file
      //if (fs.exists(new Path(path)))
       // fs.delete(new Path(path), true)

      rdd.saveAsSequenceFile(path)
   // }
  }
}

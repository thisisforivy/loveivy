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

package edb.catalog

import edb.parser._

import scala.{List=>MList,Iterator=>MIterator}
import scala.io._
import scala.reflect.BeanProperty
import scala.util.control.Breaks._
import scala.collection.mutable.{Map => MMap,Set=>MSet}
import scala.collection.JavaConversions._
import scala.compat._
import scala.util.{Random=>SRandom}

import java.util._
import java.io.{FileWriter, PrintWriter, File}
import java.io.OutputStream

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.util._

import com.google.common.io.Files

class Catalog (location: String) extends Serializable  { 

  val DEFAULT_LOCATION = "hdfs://localhost:9000/edb/catalog"
  val conf = new Configuration()
  conf.addResource(new Path("/server/hadoop/conf/core-site.xml"))
  val fs = FileSystem.get(conf)

  //init cache
  var catalogCache: Properties = null

  def initCache() {
    var loc = new Path(location)

    if (!fs.exists(loc)) {
      loc = new Path(DEFAULT_LOCATION) 
    }
    val cache = new Properties()
    cache.load(fs.open(loc))
    catalogCache =cache
  }
}

object Catalog extends Serializable {

  val conf = new Configuration()
  conf.addResource(new Path("/server/hadoop/conf/core-site.xml"))
  val fs = FileSystem.get(conf)



  def getCache = catalog.catalogCache


  def dropTable(name: String){

    val sch = getTableSchema(name)
    val loc = getTableLocation(name)
    val file = new Path(loc)
    if( sch != null) {
      val id = sch.getId
      println("dropping " + id)
      catalog.catalogCache.remove(id+".1")
      catalog.catalogCache.remove(id+"1.default")
      catalog.catalogCache.remove(id+".hdfsStorage")
      catalog.catalogCache.remove(id+".latestVersion")
      catalog.catalogCache.remove(id+".name")
      catalog.catalogCache.get(id+".name")

      //remove the schema ID from global list
      val newList = getSchemaIDs
      newList.remove(id)

      var idList: String = ""
      for (i <- newList){
        idList = idList +i + ","
      }
      idList = idList.substring(0, idList.length-1)
      catalog.catalogCache.remove("SchemaIDs")
      catalog.catalogCache.put("SchemaIDs", idList)

      persistCatalog(catalog.catalogCache)
      catalog.initCache

      if (fs.exists(file))
        fs.delete(file)

      println("table " + name + " has been dropped")
    }else 
    println("table " + name + " does not exist!")

  }

  //create a table schema in catalog
  def createTable(metaTable: CREATE_TABLE_QB) {

    val name = metaTable.name
    val eItr: MIterator[TABLE_ELEMENT] = 
    metaTable.elementList.iterator 

    //table name should not be used
    if (getTableSchema (name) != null) {
      throw new 
      EdbException("table name exists in the catalog")
    }

    val newSchemaID = getSchemaIDs.toList.sorted.last + 1
    val iter = getSchemaIDs.toList.sorted.iterator

    var schIDs = ""
    while (iter.hasNext){
      schIDs = schIDs + iter.next + ","
    }

    schIDs = schIDs + newSchemaID
    catalog.catalogCache.setProperty("SchemaIDs", schIDs)

    //set name
    var key = newSchemaID + ".name"
    var value = name
    catalog.catalogCache.setProperty(key, value)

    //set version
    key = newSchemaID + ".latestVersion"
    value = "1"
    catalog.catalogCache.setProperty(key, value)

    //set storage 
    key = newSchemaID + ".hdfsStorage"
    value = "hdfs://localhost:9000/edb/" + name
    catalog.catalogCache.setProperty(key, value)

    //set schema
    key = newSchemaID + ".1"
    value =""
    var default = ""

    while(eItr.hasNext){

      val ele = eItr.next
      val colName = ele.name
      val colType = ele.coltype

      if (eItr.hasNext) {
        value = value + colType + " " + colName+ "," 

        if (colType.toString.equals("string"))
          default = default + "Unknown" + ","
        else 
          default = default + "-1" + ","
      }
      else {
        value = value + colType + " " + colName 
        if (colType.toString.equals("string"))
          default = default + "Unknown" 
        else 
          default = default + "-1" 
      }
    }
    catalog.catalogCache.setProperty(key, value)

    //set default 
    key = newSchemaID + ".1.default"

    persistCatalog(catalog.catalogCache)
    //reset cache
    catalog.initCache

  }//end create table

  /**
    * persist catalog to a hdfs file
    */
  def persistCatalog(cache: Properties) {

    val CATALOG_LOCATION = 
    "hdfs://localhost:9000/edb/catalog"

    val conf = new Configuration()
    conf.addResource(
      new Path("/server/hadoop/conf/core-site.xml"))
    val fs = FileSystem.get(conf)

    // out.writeUTF(kvlist)
    val kvMap: scala.collection.mutable.Map[String, String]= cache

    val sortedKVs = kvMap.toSeq.sortBy(_._1)

    //add line break to bypass utf prefix, utf always have a len 
    //of string written first 
    var kvList: String = "\n"
    for (x <- sortedKVs) {
      kvList = kvList + x._1 + "="+ x._2 + "\n"
    }

    //save to local tmp file
    val tempDir = "/tmp/edb/" 
    val tmpFileLoc: String = tempDir + 
    "/newCatalog" + Platform.currentTime

    val outTmp = new FileWriter(tmpFileLoc)
    outTmp.write(kvList)
    println(tempDir + "/input")
    outTmp.close()
    val file = new File(tmpFileLoc);

    val catalogLoc = new Path(CATALOG_LOCATION)
    if (fs.exists(catalogLoc)){
      fs.delete(catalogLoc)
    }
    FileUtil.copy(file, fs, catalogLoc, true, conf) 
    //delete tmp file
    file.delete()
  }

  /**
    * Display catalog table name list
    */
  def showTables(){

    println()
    println()
    println("EDB has " + getSchemaIDs().size + " tables:")
    println("-------------------------------------")
    println()
    getSchemaIDs() map  {
      i=> println("\n" +getTableName(i))
    }

    println()
    println("-------------------------------------")

    println()
    println()
  }

  def descTable(name: String){
    println()
    println()
    println("-------------------------------------")
    println()
    println(getTableSchema (name))
    println()
    println("-------------------------------------")
    println()
    println()
  }

  //private var catalog: Properties = (new Catalog("test")).loadCatalog()
  private var catalog = new Catalog("/test")
  catalog.initCache

  //sch is uniquely identified by id_version string
  //the colIdxMap is colName->colIdx
  private val sch2ColIdxMap = 
  MMap[String, MMap[String,Int]]()

  //create tableName->latestSchema map 
  def nameToSchema = (getSchemaIDs() map 
    { i=> getTableName(i)-> new Schema(i, getTableLatestVersion(i).toByte)}).toMap

  /* return a set containing all schema IDs in the catalog */
  def getSchemaIDs(): MSet[Int] = {
    val schemaIDs = MSet.empty[Int] 
    val idList = catalog.catalogCache.getProperty("SchemaIDs", "-1");
    val ids: Array[String] = idList.split(",");

    for (id <-ids)
      schemaIDs += Integer.parseInt(id.trim()) 

    if (schemaIDs.size == 1 && schemaIDs.contains(-1)) {
      Console.err.println("cannot find schema ids")
      exit(100)
    }
    schemaIDs
  }


  def getTableName(id: Int):String = {
    catalog.catalogCache.getProperty(id+".name") 
  }

  def getTableLocation(id: Int):String = {
    catalog.catalogCache.getProperty(id+".hdfsStorage") 
  }

  def getTableLocation(tableName: String):String = {
    val id = getTableSchema(tableName).getId
    val loc = catalog.catalogCache.getProperty(id+".hdfsStorage") 
    loc
  }
  def getTableLatestVersion(id: Int):Int = {
    (catalog.catalogCache.getProperty(id+".latestVersion")).toInt
  }

  def getTableSchema (id: Int, version: Byte):String= {
    catalog.catalogCache.getProperty(id+"." + version) 
  }

  def getTableSchema (name: String): Schema = 
  if (nameToSchema contains (name)) nameToSchema(name)
    else null

  /**
    * For the current record, we use idx (start from 0)
    * to find the value of the column
    * @param t SequenceRecord
    * @param idx column idx 
    */
  def getVal(
    t: SequenceRecord, 
    idx: Int): genericValue= t.getData()(idx)



  /**
    * For the current record, we use identifier 
    * to find the value of the column
    * @param t SequenceRecord
    * @param iden column name
    * @return the edb value for the column
    *
    */
  def getVal(
    t: SequenceRecord, 
    iden: String,
    inSch: Schema): genericValue= { 

    //val sch: Schema = t.getSch()
    val sch = inSch
    assert(sch != null)



    val schKey: String = sch.getId + "_" + sch.getVersion
    var colName2IdxMap: MMap[String,Int] =
    sch2ColIdxMap.get(schKey) match {
      case Some(x)=> x
      case None => null
    }

    //first time, build colName->idx map
    if (colName2IdxMap == null){
      //loop through schema atts,build map
      colName2IdxMap = MMap[String,Int]()

      var i =0;
      for(att<- sch.getAtts()){
        colName2IdxMap += att.getName()->i
        i += 1
      }
      sch2ColIdxMap += schKey->colName2IdxMap
    }

    val vals: Array[genericValue]= t.getData()


    //for group by count(*) agg function, 
    //we want to sent 1 to reducer
    if (iden == "*")
      new IntVal("",1)
    else 
      colName2IdxMap.get(iden) match {
      case Some(x)=> vals(x)
      case None => {
        Console.err.println("att was not found for "+ 
          iden + " in "+ sch.toString)
        throw new EdbException("not supported")
      }
    }
  }

  /**
    * For the current record, we use identifier 
    * to find the col type
    * @param t SequenceRecord
    * @param iden column name
    * @return the edb type for the column
    *
    */
  def getAttType(
    iden: String,
    inSch: Schema): Attribute = { 

    //val sch: Schema = t.getSch()
    val sch = inSch
    assert(sch != null)


    val schKey: String = sch.getId + "_" + sch.getVersion
    var colName2IdxMap: MMap[String,Int] =
    sch2ColIdxMap.get(schKey) match {
      case Some(x)=> x
      case None => null
    }

    //first time, build colName->idx map
    if (colName2IdxMap == null){
      //loop through schema atts,build map
      colName2IdxMap = MMap[String,Int]()

      var i =0;
      for(att<- sch.getAtts()){
        colName2IdxMap += att.getName()->i
        i += 1
      }
      sch2ColIdxMap += schKey->colName2IdxMap
    }

    val atts: Array[Attribute]= inSch.getAtts 

    colName2IdxMap.get(iden) match {
      case Some(x)=> atts(x)
      case None => {
        Console.err.println("att was not found for "+ 
          iden + " in2 "+ sch.toString)
        throw new EdbException("not supported")


      }
    }
  }
  def getAttIdx(
    iden: String,
    inSch: Schema): Int = { 

    assert(inSch != null)
    val sch = inSch

    val schKey: String = sch.getId + "_" + sch.getVersion
    var colName2IdxMap: MMap[String,Int] =
    sch2ColIdxMap.get(schKey) match {
      case Some(x)=> x
      case None => null
    }

    //first time, build colName->idx map
    if (colName2IdxMap == null){
      //loop through schema atts,build map
      colName2IdxMap = MMap[String,Int]()

      var i =0;
      for(att<- sch.getAtts()){
        colName2IdxMap += att.getName()->i
        i += 1
      }
      sch2ColIdxMap += schKey->colName2IdxMap
    }

    val atts: Array[Attribute]= inSch.getAtts 

    colName2IdxMap.get(iden) match {
      case Some(x)=> x
      case None => {
        Console.err.println("att was not found for "+ 
          iden + " in2 "+ sch.toString)
        throw new EdbException("not supported")
      }
    }
  }

  /**
    * generate a random string of given size
    */
  def randString(size: Int) = {

    assert(size >0)
    var result: String = ""
    for (i <- 1 to size)
      result = result + SRandom.nextPrintableChar 
    result
  }

  /**
    * generate a random table based on schema
    * and cnt, store it in hdfs table location
    */
  def generateTable(name: String, cnt: Int) {

    val sch = getTableSchema (name)
    assert(sch != null)
    println(sch)

    val loc = getTableLocation(name)
    val hdfsPath = new Path(loc)

    if ( fs.exists(hdfsPath)){
      fs.delete(hdfsPath)
    }

    var key:EdbIntWritable  = new EdbIntWritable()
    var value:SequenceRecord  = new SequenceRecord()

    var writer:SequenceFile.Writer  = null
    val gen = new SRandom(Platform.currentTime)

    try{

      writer = SequenceFile.createWriter(fs, 
        conf, hdfsPath, key.getClass(),value.getClass())
      val schId = sch.getId
      val schV = sch.getVersion
      val attNum = sch.getNumAtts

      //generate cnt number of record
      for (i <- 1 to cnt){
        key.setValue(i)
        value.setSchId(schId)
        value.setSchVersion(schV)
        value.setSch(new Schema(schId, schV))
        var data: Array[genericValue] = new Array[genericValue](attNum)

        for (j <- 0 to attNum-1){
          data(j)= sch.getAtts()(j) match {
            case e1: IntAtt=> new 
            IntVal(e1.getName,gen.nextInt(100))
            case e2: DoubleAtt=> new DoubleVal(e2.getName, 
              gen.nextDouble*100000)
            case e3: FloatAtt=>new FloatVal(e3.getName,
              gen.nextFloat*1000000)
            case e4: LongAtt=> new LongVal(e4.getName,
              gen.nextInt(100000))
            case e5: StringAtt=> new StringVal(e5.getName,
              randString(10))
            case e6: BooleanAtt=> new BooleanVal(e6.getName,
              gen.nextBoolean)
            case e7: DateAtt=> new DateVal(e7.getName,
              gen.nextInt(1000000))
            case _ => {
              Console.err.println(
                "unknow att ")               
              exit(100) } 
            }
          }
          value.setData(data)
          println(value.toString)
          writer.append(key,value)
        }//end cnt loop

      } catch {
        case e: Exception => { 
          Console.err.println(
            "exception throwed " + e)               
          exit(100)  
        }
      } finally {
        IOUtils.closeStream(writer)
      } 
    }//end generateTable
  }

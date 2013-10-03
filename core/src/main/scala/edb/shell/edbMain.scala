package edb.shell
/* SQLParser object is in edb.parser package */
import edb.parser._
import edb.catalog._
import edb.engine._
import edb.enviroment._

import spark.SparkContext
import SparkContext._
import scala.io._
import scala.util.Random

import org.apache.hadoop.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._


object edbMain {

  def main(args: Array[String]) {

    //init sc 
    EdbEnv.init()
    //compile query to an operator tree
    val opTree = 
    SQLParser.compile(args.reduceLeft[String](_ + '\n' + _))
    //init all operators
    opTree.initializeMasterOnAll
    //kicks off query
    val result = opTree.execute
    result.collect.map(x=>println(x))


    /*
     //offline_media(int adv_id,date day,int zip_code, String media_type,double media_spend
       // val DEFAULT_LOCATION = "hdfs://localhost:9000/edb/offline_media"
       // val DEFAULT_LOCATION = "hdfs://localhost:9000/edb/online_media"
       //online_media(int advertiser_id, date ts, int media_channel_id,int zipcode, int impression,int click)
       //result: int adv_id,int zipcode,int sum_media_spend, double ratio;adv_id
       val DEFAULT_LOCATION = "hdfs://localhost:9000/edb/media_provider_adv_metadata"
       val conf = new Configuration()
       conf.addResource(new Path("/server/hadoop/conf/core-site.xml"))
       val fs = FileSystem.get(conf)
       val loc = new Path(DEFAULT_LOCATION)

       //  println(Catalog.getTableSchema("product_data_platform.media_provider_cr_metadata"))
       //  SimpleJob.run()
       var key:EdbIntWritable  = new EdbIntWritable()
       var value:SequenceRecord  = new SequenceRecord()

       var writer:SequenceFile.Writer  = null

       try{

         writer = SequenceFile.createWriter(fs, conf, loc, key.getClass(),value.getClass())
         var a = new Random(2312)

         for (i <- 0 to 20){

           key.setValue(i)

           value.setSchId(5)
           value.setSchVersion(1)
           value.setSch(new Schema(5,1))
           var data: Array[genericValue] = new Array[genericValue](4)
           data(0)= new LongVal("advertiser_id", i)
           data(1)= new LongVal("zip", a.nextInt(10000))
           data(2)= new StringVal("sum_m", "adv_" + a.nextInt(100))
           //data(1)= new DateVal("ts", a.nextInt(1000000))
           //data(2)= new IntVal("media_channel_id", a.nextInt(10))
           //data(3)= new IntVal("zip", a.nextInt(100000))
           //data(3)= new StringVal("media_channel_id", a.nextInt(10))
           //data(4)= new IntVal("imp", a.nextInt(100000))
           //data(5)= new IntVal("clk", a.nextInt(100))
           value.setData(data)
           println(value.toString)
           writer.append(key,value)
         }

       } catch {
         case e: Exception => { 
           Console.err.println("exception throwed " + e)               
           exit(100)  
         }
       } finally {
         IOUtils.closeStream(writer)
       } 

       println("***************************************")
       println("***************************************")
       println("***************************************")
       println("***************************************")

       var reader: SequenceFile.Reader = null

       try{

         reader = new SequenceFile.Reader(fs, loc, conf)

         var key2:EdbIntWritable  = new EdbIntWritable()
         var value2:SequenceRecord  = new SequenceRecord()

         while(reader.next(key2,value2)){
           println("reading " + key2.toString + "," + value2.toString)
         }
       } catch {
         case e: Exception => { 
           Console.err.println("exception throwed " + e)               
           exit(100)  
         }
       } finally {
         IOUtils.closeStream(reader)
       }  
     } */
    /*
     val sc = new SparkContext("local", "Simple Job", "/server/spark",
       List("/Users/mwu/edb/core/target/scala-2.9.3/simple-project_2.9.3-1.0.jar"))


     val file = new DBFile()
     val sch = new Schema(5,1)
     file.init("hdfs://localhost:9000/edb/file1", sch, new SequenceDBFile(), sc)
     val r = file.link(true)

     val loc = "hdfs://localhost:9000/edb/file2"
     file.save(loc) */
    //create Spark context
    /*
     EdbEnv.init()

     //create table scan operator
     val tsc: TableScanOperator = new TableScanOperator("product_data_platform.media_provider_adv_metadata")

     //add Project operator
     val idx = Array(1,2)
     val outSch = new Schema(8,1)
     val inSch = new Schema(5,1)
     val p = new ProjectOperator(inSch,outSch,null, idx)
     p.addParent(tsc)

     //add Filter operator 
     val e = new PRED_EQUAL(
       new IDENTIFIER("advertiser_id"), new NUMBER(3207))
     val e1= new FilterOperator(outSch,outSch,e,null)
     e1.addParent(p)

     //invoke the top operator , execute the operator tree
     e1.initializeMasterOnAll
     val mm = e1.execute
     mm.collect.map(x=>println(x)) */

    //create Spark context

    //  println(SimpleJob.run(0))
    //val input = Source.fromFile("/Users/mwu/input.sql").getLines.reduceLeft[String](_ + '\n' + _)
    //SQLParser.compile(input)

    // read from file 
    //val input = Source.fromFile("/Users/mwu/input.sql").
    //getLines.reduceLeft[String](_ + '\n' + _)

  }
}

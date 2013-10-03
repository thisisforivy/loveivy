package edb

import edb.parser._
import edb.catalog._
import edb.engine._

import spark.SparkContext
import SparkContext._
import spark.{RDD}
import scala.io.Source

import scala.reflect.BeanProperty

import java.io.{FileWriter, PrintWriter, File}
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.hadoop.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.mapred._
import org.apache.hadoop.fs._
import org.apache.hadoop.util._
import com.google.common.io.Files


@RunWith(classOf[JUnitRunner])
class testSuite extends FunSuite {

  val sc = new SparkContext("local", "Simple Job", "/server/spark",
    List("/Users/mwu/edb/core/target/scala-2.9.3/simple-project_2.9.3-1.0.jar"))

  //parser test suite starts
  test("select clause") {
    assert(SQLParser.buildAST("select a, b from d") ===    "SELECT_QB(SELECTLIST(List(IDENTIFIER(a), IDENTIFIER(b))),FROMLIST(List(TABLE(d))),None,None,None,None)")
    }

    test("select from clause ") {
      println("AST " + SQLParser.buildAST("select a, b from d as c, m as b"))
    assert(SQLParser.buildAST("select a, b from d as c, m as b") === "SELECT_QB(SELECTLIST(List(IDENTIFIER(a), IDENTIFIER(b))),FROMLIST(List(AS_TABLE(d,IDENTIFIER(c)), AS_TABLE(m,IDENTIFIER(b)))),None,None,None,None)")
    }

    test("catalog: get schema IDs"){
      assert(!Catalog.getSchemaIDs()(0))
      assert(Catalog.getSchemaIDs()(1))
      assert(Catalog.getSchemaIDs()(2))
      assert(Catalog.getSchemaIDs()(3))
    }

    test("catalog: get table name"){
      assert(Catalog.getTableName(1) === "product_data_platform.media_provider_io_metadata")
    }

    test("catalog: get table latest version"){
      assert(Catalog.getTableLatestVersion(1) == 1)
    }

    test("catalog: get table location"){
      assert(Catalog.getTableLocation(1) === "/oracle/product_data_platform")
    }

    test("catalog: get table schema"){
      assert(Catalog.getTableSchema(1,1).contains("media_provider_id"))
    }

    test("schema class: createSchema "){
      val a = new Schema(1,1)
      assert(a.toString === "product_data_platform.media_provider_io_metadata(long media_provider_id,long insertion_order_id,string name,int start_date,int end_date,float budget,int goal_type,float goal_amount;media_provider_id,insertion_order_id)")
    }

    test("schema class: equal"){
      val a = new Schema(1,1)
      val b = new Schema(1,1)
      val c = new Schema(2,1)
      assert(a==b)
      assert(a!=c)
      val d = new Schema()
      val e= Array(0,2,3)
      d.setSchema(e,a)
      assert(d.toString.equals("null(long media_provider_id,string name,int start_date;)"))

      val f = c.copy() 
      assert(f==c)
    }


    test("att: check equal"){
      val a = new IntAtt("abc")
      val a2 = new IntAtt("abce")
      val b = new FloatAtt("abc")
      val b2 = new FloatAtt("abce")
      val c = new IntAtt("abc")
      val c2 = new IntAtt("abce")
      val d = new DoubleAtt("abc")
      val d2 = new DoubleAtt("abce")
      val e = new DateAtt("abc")
      val e2 = new DateAtt("abcf")
      val f = new StringAtt("abc")
      val f2 = new StringAtt("abcf")
      val g = new StringAtt("abcd")
      assert(a!=b)
      assert(d!=b)
      assert(a!=d)
      assert(a == c)
      assert(e != f)
      assert(f != g)
      assert(f != a)
      assert(a != a2)
      assert(b != b2)
      assert(c != c2)
      assert(d != d2)
      assert(e != e2)
      assert(f != f2)
      assert(a == a)
      assert(a != null)
      assert(b == b)
      assert(b != null)
      assert(e != null)
      assert(e == e)
      assert(f == f)
      assert(f != null)

    }

    test("val: check equal"){
      val a = new IntVal("abc", 1)
      val a2 = new IntVal("abce",1)
      val a3 = new IntVal("abc",2)
      val b = new FloatVal("abc",1.0f)
      val b2 = new FloatVal("abce",1.0f)
      val c = new IntVal("abc",1)
      val c2 = new IntVal("abce",1)
      val d = new DoubleVal("abc",2.0)
      val d2 = new DoubleVal("abce",2.0)
      val e = new DateVal("abc",22323)
      val e2 = new DateVal("abcf",22323)
      val f = new StringVal("abc","abd")
      val f2 = new StringVal("abcf","abd")
      val g = new StringVal("abcd","efg")
      assert(a!=b)
      assert(d!=b)
      assert(a!=d)
      assert(a == c)
      assert(e != f)
      assert(f != g)
      assert(f != a)
      assert(a != a2)
      assert(b != b2)
      assert(c != c2)
      assert(d != d2)
      assert(e != e2)
      assert(f != f2)
      assert(a != a3)
      assert(b == b)
      assert(b != null)
    }

    test("catalog: getTableSchema()"){

      val a = new Schema(4,1)
      val b = Catalog.getTableSchema("product_data_platform.media_provider_cr_metadata") 
      assert(b== a)
      assert(a== a)
      assert(a!=null)

    }
    //parser test suite ends

    /************************************************************************
    ************************************************************************/
  //engine test suite starts

  /**
  * In this test, we use local fs to test save sequencefile
  * b/c we found use sbt/test has slow connection with hdfs
  * so, sometime it hangs. If we call save to hdfs uri in 
  * edbMain(), it works
  *
  */
  test("DBFile: the class"){

    val file = new DBFile()
    val sch = new Schema(5,1)
    file.init("hdfs://localhost:9000/edb/file1", sch, new SequenceDBFile(), sc)
    val r = file.link(true)

    //google io library. test save()
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    println("outputDir: " + outputDir)
    file.save(outputDir)

    //test getRdd()
    val result=  file.getRdd.collect() 
    assert(result(0).toString.equals("(0,[null:long 0],[null:long 3097],[null:string adv_63])"))
    assert(result(1).toString.equals("(1,[null:long 1],[null:long 3207],[null:string adv_1])"))
  }

  test("type operator "){
      val a = new IntVal("abc", 1)
      val a2 = new IntVal("abce",2)
      val a3 = a + a2
      val a4 = new IntVal("as",3)
      assert(a3 eqs a4)
      assert(a2>a)
      val a7 = new IntVal("aece",2)
      assert(a2.eqs(a7))
      assert(a2.neqs(a))
  }
}

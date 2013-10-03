package edb.engine

import edb.parser._
import edb.catalog._
import spark.SparkContext
import SparkContext._
import spark.{RDD}
import spark.{Logging}
import scala.reflect.BeanProperty
import scala.collection.mutable.ArrayBuffer

class ProjectOperator(
  inSchema: Schema, 
  outSchema: Schema,  
  expList: List[EXPRESSION]) extends 
UnaryOperator[SequenceRecord] {

  //setup operator meta data
  override def initializeOnMaster() {
    assert(!Option(expList).isEmpty)
    setInSch(inSchema)
    setOutSch(outSchema)
  }

  /** 
    * This function evaluate a the expList on 
    * a given tuple, and return a new SequenceRecord
    * with the expression value in it
    *
    */
  def evalFunc (tuple: Any): SequenceRecord= {

      new SequenceRecord(
      //for each expression, evaluate it
        expList.map(
          x => 
          Utils.eval(x, 
            tuple.asInstanceOf[SequenceRecord],
            inSchema)).toArray 
        )
    }

    override def processPartition(index: Int, 
      iter: Iterator[_]): Iterator[_] = {
      iter.map(row => evalFunc(row))
    }
  }


package edb.engine

import edb.parser._
import edb.catalog._
import spark.SparkContext
import SparkContext._
import spark.{RDD}
import spark.{Logging}
import scala.reflect.BeanProperty
import scala.collection.mutable.ArrayBuffer

class FilterOperator(inSchema: Schema, 
  outSchema: Schema, 
  pred: PREDICATE, 
  attList: Array[Int]) extends 
UnaryOperator[SequenceRecord]{

  private var schema: Schema = null 

  def filterFunc(tuple: Any): Boolean= {
    Utils.inspector(
      tuple.asInstanceOf[SequenceRecord],pred, inSchema)
  }

  //setup operator meta data
  override def initializeOnMaster() {
    assert(pred!= null)
    setInSch(inSchema)
    setOutSch(outSchema)
  }

  override def processPartition(index: Int, 
    iter: Iterator[_]): Iterator[_] = { 
    iter.filter(filterFunc)
  }

  override def toString(): String = 
     "FILTER--predicate: " + pred 
 
}


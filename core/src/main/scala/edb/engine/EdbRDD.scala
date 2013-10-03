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
/**
  * Wrapper around RDD for EDB
  */
class  EdbRDD (
  _sch: Schema,
  _rdd: RDD[SequenceRecord])
{
  @BeanProperty var sch = _sch
  @BeanProperty val rdd = _rdd
}

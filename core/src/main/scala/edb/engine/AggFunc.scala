package edb.engine

import edb.parser._
import edb.catalog._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD}
import org.apache.spark.{Logging}
import scala.reflect.BeanProperty
import scala.collection.mutable.ArrayBuffer

abstract class AggFunc extends Logging with Serializable {
  /**
    * init state of agg func
    */
  def init

  /**
    * update state using current tuple
    */
  def accumulate(tuple: Any)

  def terminate: genericValue 

  //print out the operator and related data
  def toString: String

  //given an att, init a generic val
  final def initVal (valType: Attribute): genericValue = {
    valType match {
      case a1: IntAtt =>  new IntVal("", 0)
      case a2: FloatAtt =>  new FloatVal("", 0.0f)
      case a3: DoubleAtt =>  new DoubleVal("", 0.0)
      case a4: LongAtt =>  new LongVal("", 0l)
      case _ =>throw new   
      EdbException("not supported agg type in Sum")
    }
  }
}


case class sum(
  att: Attribute, 
  exp: EXPRESSION,
  sch: Schema) extends AggFunc {

  private var sum: genericValue = null

  override def init = {
    sum = initVal(att) 
  }

  override def accumulate(tuple: Any){
    sum =  sum + Utils.eval(exp, tuple.asInstanceOf[SequenceRecord],
      sch) 
  }
  override def terminate: genericValue = sum
}


case class count() extends AggFunc {

  private var cnt: Long = 0l

  override def init = {
    cnt = 0l
  }

  override def accumulate(tuple: Any){
    cnt = cnt + 1
  }

  override def terminate: genericValue = new LongVal("", cnt)
}

case class avg (
  att: Attribute, 
  exp: EXPRESSION,
  sch: Schema) extends AggFunc {

  private var sum: genericValue = null
  private var cnt: Long = 0l

  override def init = {
    sum = initVal(att) 
    cnt = 0l
  }

  override def accumulate(tuple: Any){
    sum =  sum + Utils.eval(exp, tuple.asInstanceOf[SequenceRecord],
      sch) 
    cnt = cnt +1
  }

  override def terminate: genericValue = {
    if (cnt >0 )
      sum /new LongVal("",cnt)
    else new DoubleVal("", 0.0)
  }
}

case class max ( 
  att: Attribute, 
  exp: EXPRESSION,
  sch: Schema) extends AggFunc {

  private var maxx: genericValue = null
  private var firstProcessed: Boolean = false

  override def init = {
    firstProcessed = false 
  }

  override def accumulate(tuple: Any){

    if (!firstProcessed) {
      maxx = Utils.eval(exp, tuple.asInstanceOf[SequenceRecord],
        sch) 
      firstProcessed = true
    }
    else { 
      val cur = Utils.eval(exp, tuple.asInstanceOf[SequenceRecord],
        sch) 
      if (cur > maxx) maxx =cur 
    }
  }
  override def terminate: genericValue = maxx
}

case class min ( 
  att: Attribute, 
  exp: EXPRESSION,
  sch: Schema) extends AggFunc {

  private var minn: genericValue = null
  private var firstProcessed: Boolean = false

  override def init = {
    firstProcessed = false 
  }

  override def accumulate(tuple: Any){

    if (!firstProcessed) {
      minn = Utils.eval(exp, tuple.asInstanceOf[SequenceRecord],
        sch) 
      firstProcessed = true
    }
    else { 
      val cur = Utils.eval(exp, tuple.asInstanceOf[SequenceRecord],
        sch) 
      if (cur<minn) minn= cur
    }
  }
  override def terminate: genericValue = minn
}


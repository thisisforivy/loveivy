/* 
 This is the value of SequenceInputFormat. 
 The key of SequenceInputFormat is the byteArrayWriable of expression list 

 - schema ID: int
 - schema Version: byte
 - data : array of genericValue

 - readFields: StreamIn
 - write: StreamOut

 - equals

 - toString

 */
package edb.catalog

import scala._
import scala.io._
import scala.reflect.BeanProperty
import scala.util.control.Breaks._

import org.apache.hadoop.io.{WritableComparable}

import java.util._
import java.io.DataInput
import java.io.DataOutput
/**
  * A wrapper on Hadoop IntWritable. It implements Serializable
  * for spark framework
  *
  */
class EdbIntWritable extends  WritableComparable[Any] with Serializable {

  @BeanProperty  var value: Int = _

  /* auxiliary constructor */
  def this(in: Int) {
    //must call the default constructor
    this()
    setValue(in)
  }

  override def readFields(in: DataInput){
    setValue(in.readInt())
  }

  /* serialize data */ 
  override def write(out: DataOutput){
    out.writeInt(getValue)
  }

  override def equals(other: Any): Boolean = other.isInstanceOf[EdbIntWritable] && 
  other.asInstanceOf[EdbIntWritable].getValue == getValue

  override def hashCode: Int = value.hashCode

  override def toString = value.toString

  /**
    * This function make current SequenceRecord copy
    * the fromMe record
    * @param fromMe the src SequenceRecord instance
    * @return nothing
    *
    */
  def copy(): EdbIntWritable = new EdbIntWritable(getValue)

  /**
    *
    * @param c the cutoff for testing RDD.filter
    *
    */
  def GT(c: Int): Boolean = value > c

  def compareTo(o: Any): Int = {
    val thatValue = o.asInstanceOf[EdbIntWritable].getValue 
    if(value < thatValue) -1 else if (value == thatValue) 0 else 1
  }

}//end class



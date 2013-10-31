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

import scala._
import scala.io._
import scala.reflect.BeanProperty
import scala.util.control.Breaks._

import java.util._
import java.io.DataInput
import java.io.DataOutput

import org.apache.hadoop.io.{WritableComparable}

/* 
 This is the Key of SequenceInputFormat. 
 The value of SequenceInputFormat is the byteArrayWriable of expression list 

 - schema ID: int
 - schema Version: byte
 - data : array of genericValue

 - readFields: StreamIn
 - write: StreamOut

 - equals

 - toString

 */

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



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

package edb.engine

import edb.parser._
import edb.catalog._

import scala.reflect.BeanProperty
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD}
import org.apache.spark.{Logging}


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

  override def toString(): String = 
     "PROJECTION--expression list: " + expList 
 
  }


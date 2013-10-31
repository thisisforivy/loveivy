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

class GroupByPreShuffleOperator(
  inSchema: Schema, 
  outSchema: Schema, 
  gbyExpList: List[EXPRESSION],
  aggExpList: List[EXPRESSION]) 
extends UnaryOperator[SequenceRecord] {

  private var aggInnerExpList: List[EXPRESSION]= _

  //setup operator meta data
  override def initializeOnMaster() {
    assert(!Option(aggExpList).isEmpty)
    //when group by clause is present
    //gbyExpList is non-empty
    //for select count(*) we do not have
    //gby clause
    assert(!Option(gbyExpList).isEmpty)

    //create col or '*' expression from agg(ident)
    def innerExp (exp: EXPRESSION): EXPRESSION = {
      exp match {
        case a1: SUM_EXP => a1.e
        case a2: AVG_EXP => a2.e
        case a3: MIN_EXP => a3.e
        case a4: MAX_EXP => a4.e
        case a5: CNT_EXP => a5.e
        case _ =>throw new 
        EdbException("not supported agg func ")
      }
    }

    //extract the inner exp of agg func
    aggInnerExpList= aggExpList.map(x=> innerExp(x))

    //the schema of input record
    setInSch(inSchema)
    setOutSch(outSchema)
  }

  /** 
    * This function convert the expList on 
    * a given tuple to a <k,v> pair
    *
    */
  def evalFunc (tuple: Any): 
  (SequenceRecord, 
    SequenceRecord) = {

    //create (groupbyExpr, aggInnerExp)
      (new SequenceRecord(
      //for each expression, evaluate it
        gbyExpList.map(
          x => 
          Utils.eval(x, 
            tuple.asInstanceOf[SequenceRecord],
            inSchema)).toArray 
        ),
      new SequenceRecord(
      //for each expression, evaluate it
        aggInnerExpList.map(
          x => 
          Utils.eval(x, 
            tuple.asInstanceOf[SequenceRecord],
            inSchema)).toArray 
        ))
    }

  /**
    * Reducer side aggregation
    */
  override def processPartition(
    split: Int, 
    iter: Iterator[_]) =  iter.map(row => evalFunc(row))

  override def toString(): String = 
     "GBYPRESHUFFLE--gby expression list: " + 
     gbyExpList + ", agg expression list: " + 
     aggExpList

}

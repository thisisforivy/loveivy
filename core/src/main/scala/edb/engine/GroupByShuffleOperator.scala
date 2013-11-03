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

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.{Aggregator, HashPartitioner}
import org.apache.spark.rdd.{RDD, ShuffledRDD,PairRDDFunctions}
import org.apache.spark.rdd._
import org.apache.spark.{Logging}


class GroupByShuffleOperator(
  aggSchema: Schema, 
  outSchema: Schema, 
  numReducer: Int,
  gbyExpList: List[EXPRESSION],
  aggExpList: List[EXPRESSION])
extends UnaryOperator[SequenceRecord] {

  private var aggFuncs: List[AggFunc] = _

  //setup operator meta data
  override def initializeOnMaster() {
    assert(numReducer >= 1)
    assert(!Option(aggExpList).isEmpty)
    //when group by clause is present
    //gbyExpList is non-empty
    //for select count(*) we do not have
    //gby clause
    assert(!Option(gbyExpList).isEmpty)
    setInSch(aggSchema)
    setOutSch(outSchema)

    aggFuncs = aggExpList.map(
      x=> x match {
        case a1: SUM_EXP=> { 
          val att = Catalog.getAttType(a1.e.asInstanceOf[IDENTIFIER].iden, inSch)
          new sum(att ,a1.e, inSch)
        }
        case a2: CNT_EXP=> { 
          new count()
        }
        case a3: AVG_EXP=> { 
          val att = Catalog.getAttType(a3.e.asInstanceOf[IDENTIFIER].iden, inSch)
          new sum(att ,a3.e, inSch)
        }
        case a4: MIN_EXP=> { 
          val att = Catalog.getAttType(a4.e.asInstanceOf[IDENTIFIER].iden, inSch)
          new min(att ,a4.e, inSch)
        }
        case a5: MAX_EXP=> { 
          val att = Catalog.getAttType(a5.e.asInstanceOf[IDENTIFIER].iden, inSch)
          new max(att ,a5.e, inSch)
        }
        case _ => throw new EdbException("not supported agg func " + x)
      })
  }

  /**
    * Do shuffling, create RDD
    */
  override def preprocessRdd(rdd: RDD[_]): RDD[_] = {

    rdd.asInstanceOf[RDD[(Any, Any)]].combineByKey(
      GroupByAggregator.createCombiner _,
      GroupByAggregator.mergeValue _,
      null,
      new HashPartitioner(numReducer), false) 
  }

  /**
    * Reducer side aggregation
    */
  override def processPartition(
    split: Int, iter:Iterator[_]) = 
  { 
    val newIter = iter.map { 
      case (key: SequenceRecord, values: Seq[_]) =>   
      //init agg funcs
      aggFuncs.map(x=>x.init)

      //accumulate the group values for current key
      values.foreach( v => aggFuncs.map(y=>y.accumulate(v)))

      //prepare the current key output tuple
      val aggs: List[genericValue]= aggFuncs.map(x => x.terminate)

      val output: Array[genericValue] = 
      key.asInstanceOf[SequenceRecord].getData ++ 
      aggs.toArray[genericValue]
      new SequenceRecord(output)
    }
    newIter
  }
  override def toString(): String = 
  "GBYSHUFFLE--gby expression list: " + 
  gbyExpList + ", agg expression list: " + 
  aggExpList + ", reducer#: " + numReducer
}

object GroupByAggregator {
  def createCombiner(v: Any) = ArrayBuffer(v)
  def mergeValue(buf: ArrayBuffer[Any], v: Any) = buf += v
}

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


abstract class Operator[T] extends Logging with Serializable {

  @transient private val _childOperators = 
    new ArrayBuffer[Operator[_]]()
  @transient private val _parentOperators = 
    new ArrayBuffer[Operator[_]]()
    //input RDD schema
  @BeanProperty var outSch: Schema = _
    //output RDD schema
  @BeanProperty var inSch: Schema = _

  /**
   * Recursively calls initializeOnMaster() for the
   * entire query plan. Parent
   * operators are called before children.
   * setup config. this is invoked by terminal operator
   */
  def initializeMasterOnAll() {
    _parentOperators.foreach(_.initializeMasterOnAll())
    initializeOnMaster()
  }


  /**
   * Initialize the operator on master node. This can have 
   * dependency on other
   * nodes. When an operator's initializeOnMaster() is invoked, 
   * all its parents'
   * initializeOnMaster() have been invoked.
   */
  def initializeOnMaster() {}

  /**
   * Initialize the operator on slave nodes. This method should have no
   * dependency on parents or children. Everything that is not 
   * used in this
   * method should be marked @transient.
   */
  def initializeOnSlave() {}


  /**
    * Return the join tag. This is usually just 0. 
    * ReduceSink might set it to
    * something else.
    */
  def getTag: Int = 0

  def processPartition(split: Int, iter: Iterator[_]): Iterator[_]

  /**
    * Execute the operator. This should recursively execute parent operators.
    */
  def execute(): RDD[_]

  //after operator
  def childOperators = _childOperators
  //precedent operator
  def parentOperators = _parentOperators

  def addParent(parent: Operator[_]) {
    _parentOperators += parent
    parent.childOperators += this
  }

  def addChild(child: Operator[_]) = child.addParent(this)

  protected def executeParents(): Seq[(Int, RDD[_])] = {
    parentOperators.map(p => (p.getTag, p.execute()))
  }

  //print out the operator and related data
  def toString: String

}

object Operator extends Logging {

  /**
    * Calls the code to process the partitions. It is placed here because we want
    * to do logging, but calling logging automatically adds a reference to the
    * operator (which is not serializable by Java) in the Spark closure.
    */
  def executeProcessPartition(
    operator: Operator[_], 
    rdd: RDD[_]): RDD[_] = {
    rdd.mapPartitionsWithIndex { case(split, partition) =>
    //logDebug("Started executing mapPartitions for operator: " + operator)
    val newPart = operator.processPartition(split, partition)
    //logDebug("Finished executing mapPartitions for operator: " + operator)
    newPart}
  }
}

/**
  * A base operator class that has at most one parent.
  * Operators implementations should override the following methods:
  *
  * preprocessRdd: Called on the master. Can be used to transform the RDD before
  * passing it to processPartition. For example, the operator can use this
  * function to sort the input.
  *
  * processPartition: Called on each slave on the output of preprocessRdd.
  * This can be used to transform rows into their desired format.
  *
  * postprocessRdd: Called on the master to transform the output of
  * processPartition before sending it downstream.
  *
  */
abstract class UnaryOperator[T] extends Operator[T] {

  /** Process a partition. Called on slaves. */
  def processPartition(split: Int, iter: Iterator[_]): Iterator[_]

  /** Called on master. */
  def preprocessRdd(rdd: RDD[_]): RDD[_] = rdd

  /** Called on master. */
  def postprocessRdd(rdd: RDD[_]): RDD[_] = rdd

  def parentOperator = parentOperators.head

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null
    val rddPreprocessed = preprocessRdd(inputRdd)
    val rddProcessed = Operator.executeProcessPartition(this, rddPreprocessed)
    postprocessRdd(rddProcessed)
  }
}

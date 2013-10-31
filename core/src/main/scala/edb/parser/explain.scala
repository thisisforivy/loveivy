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

package edb.parser

import edb.engine._
import edb.catalog._

import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.token._
import scala.io._

import org.apache.spark.{Logging}

/**
  * Singleton class doing explain plan
  */
object Explain extends Logging {

  def explain(optree: Operator[SequenceRecord], level: Int) {

    assert(optree!= null)

    if (level ==0){
      println()
      println()
      println("************** EDB plan starts **************") 
      println()
    }

    println("--"*level + optree)
    val parentOperators = optree.parentOperators
    if (parentOperators.size>0){
      println("     "*level + "|")
      parentOperators.map(
        x=>
        explain(x.asInstanceOf[Operator[SequenceRecord]], 
          level+1))
      }


      if (level ==0){
        println()
        println("************** EDB plan ends **************") 
        println()
        println()
      }
    }//end explain()

  }//end Explain



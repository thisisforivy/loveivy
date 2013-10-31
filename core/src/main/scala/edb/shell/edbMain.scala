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

package edb.shell

import edb.parser._
import edb.catalog._
import edb.engine._
import edb.enviroment._

import scala.io._
import scala.Console._
import scala.util.Random

import org.apache.spark.SparkContext._
import org.apache.hadoop.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._


object edbMain {

  def main(args: Array[String]) {

    var ok = true
    print("edb> ")

    while( ok ) {
      val ln = readLine()
      ok = ln != "quit" 
      if( ok ) {
        val input: Array[String]= Array(ln)
        try{
          //init sc for each query
          val opTree = 
          SQLParser.compile(input.reduceLeft[String](_ + '\n' + _))
          if(opTree!= null){

            EdbEnv.init()
            opTree.initializeMasterOnAll
            //kicks off query
            val result = opTree.execute
            val b = result.collect()

            //clear screen
            for (i <-1 to 100)
              println()
            b.map(x=>println(x))   

            EdbEnv.stop()
          }
          println()
          print("edb> ")
        }
        catch {
          case e: Exception => { 
            println("hey, watch out!" + e)
            print("edb> ")
          }
          case e2: RuntimeException => {
            println("hey, watch out!" + e2)
            print("edb> ")
          }
        }

      }//end ok
    }//end while
  }
}

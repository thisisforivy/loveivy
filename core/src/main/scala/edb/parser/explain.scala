package edb.parser

import edb.engine._
import edb.catalog._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.token._
import scala.io._
import spark.{Logging}

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



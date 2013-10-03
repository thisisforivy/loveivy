package edb.engine

import edb.catalog._
import edb.parser._

import spark.SparkContext
import SparkContext._
import spark.{RDD}
import spark.{Logging}

import scala.reflect.BeanProperty
import org.apache.hadoop.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.mapred._
import org.apache.hadoop.fs._
import org.apache.hadoop.util._

/**
  * Various utility methods used by EDB
  */
private object Utils extends Logging with Serializable {

  /**
    * This helper func inspect a predicate on
    * the current tuple to decide if the tuple
    * satsify the predicate
    *
    * @param tuple the tuple to be inspected
    * @param tree the predicate tree
    *
    */
  def inspector(
    tuple:SequenceRecord,
    tree: PREDICATE,
    inSch: Schema): Boolean = 
  tree match {
    case PRED_AND (p1, p2)=> inspector(tuple, p1, inSch) &&  
    inspector (tuple, p2, inSch)
    case PRED_OR (p1, p2)=> inspector(tuple, p1, inSch) ||
    inspector(tuple, p2, inSch)
    case PRED_EQUAL (e1, e2)=> 
    eval(e1,tuple, inSch) eqs eval(e2,tuple, inSch)
    case PRED_NOTEQUAL(e1, e2)=> 
    eval(e1,tuple, inSch) neqs eval(e2, tuple, inSch)
    case PRED_LESS(e1, e2)=> 
    eval(e1,tuple, inSch) < eval(e2, tuple, inSch)
    case PRED_GREATER(e1, e2)=>
    eval(e1,tuple, inSch) > eval(e2, tuple, inSch)
    case PRED_LE (e1, e2) => 
    eval(e1,tuple, inSch) <= eval(e2, tuple, inSch)
    case PRED_GE (e1, e2) => 
    eval(e1,tuple, inSch) >= eval(e2, tuple, inSch)
  }

  /**
    * This function evalute a given expression (computable
    * expression) on the given tuple, it returns a 
    *  genericValue so that all comparison will 
    * be in the
    * EDB type system. EDB type (genericValue) has 
    * implemented the following comparison op 
    * >, >=,<, 
    * <=, eqs, neqs
    */
  def eval(
    e: EXPRESSION, 
    t: SequenceRecord,
    inSch: Schema):genericValue= 
  e match {

    //look up record att
    case IDENTIFIER (iden) => Catalog.getVal(t, iden, inSch)
    //number so far is only integer
    case NUMBER(lit)=> new IntVal("", lit)
    case ADD(e1,e2) => eval(e1,t, inSch) + eval(e2,t, inSch)
    case SUBTRACT(e1,e2) => eval(e1,t, inSch) - eval(e2,t, inSch)
    case DIV(e1,e2)=> eval(e1,t, inSch) / eval(e2,t, inSch)
    case MUL(e1,e2)=> eval(e1,t, inSch) * eval(e2,t, inSch)
    case AS_EXP(e, a)=> eval(e,t,inSch)
    case _ =>  {
      Console.err.println("Unsupported evaluation on" +
        " expression "+ e.toString)
      exit(100)
    }
  }
}



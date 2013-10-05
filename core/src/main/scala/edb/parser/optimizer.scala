package edb.parser

import edb.engine._
import edb.catalog._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.token._
import scala.io._
import spark.{Logging}

object Optimizer extends Logging {

  /**
    * public API of Optimizer
    * Generate Query plan based on the parse tree
    */
  def optimize(tree: QUERYBLOCK): Operator[SequenceRecord] = {

    tree match {
      case qb: SELECT_QB => 
      optimizeSelQB(qb.asInstanceOf[SELECT_QB])
      case _ => {println("unknown qb") 
      null}
    }
  }

  /**
    * Given an AST of select qb, 
    * generate an optimal operator tree
    */
  private def optimizeSelQB(qb: SELECT_QB): Operator[SequenceRecord] = {

    val fromList = qb.fromlist.fromlist
    val predTree = if (qb.predtree.isEmpty) null 
    else qb.predtree.get

    var ret: Operator[SequenceRecord]= null 

    val selList = qb.sellist.sellist

    if (fromList.size == 1) {

      val tableName = fromList(0) match {
        case t: TABLE => t.iden
        case tas: AS_TABLE => tas.iden
        case _ => 
        throw new EdbException("not supported table type" )
      }

      val tscOperator = new TableScanOperator(tableName)

      //when predicate is not empty
      //add Filter operator as child of tsc operator
      println("getting " + tableName)
      val sch = Catalog.getTableSchema(tableName)
      assert(sch != null)

      if (predTree != null) {

        val filterOperator= new 
        FilterOperator(sch,sch,predTree, null)                    

        filterOperator.addParent(tscOperator)

        //"select *" case
        if(selList.size == 1 && 
          selList(0).isInstanceOf[STAR]){
          ret = filterOperator
        } else {
          //non STAR selection
          val projectOperator = new 
          ProjectOperator (sch,sch,selList)

          projectOperator.addParent(filterOperator)
          ret = projectOperator
        }
      } else {
        //empty predicate, create project operator

        //"select *" case
        if(selList.size == 1 && 
          selList(0).isInstanceOf[STAR]){
          ret = tscOperator 
        } else {
          val projectOperator = new 
          ProjectOperator (sch,sch,selList)
          projectOperator.addParent(tscOperator)
          ret = projectOperator
        }
      }

      //add Project operator as child of tscOperator
      //tscOperator
    }
    ret
  }
}




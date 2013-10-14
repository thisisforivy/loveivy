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
    val predTree = qb.predtree.getOrElse(null)

    var ret: Operator[SequenceRecord]= null 
    val selList = qb.sellist.sellist

    val grpbyList = qb.grpbylist.getOrElse(null)
    var gbyExpList: List[EXPRESSION] = null 
    var aggExpList: List[EXPRESSION] = List[EXPRESSION]() 

    //check whether gby list is empty or not
    if (grpbyList != null) 
      gbyExpList = grpbyList.grpbylist

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
      val sch = Catalog.getTableSchema(tableName)
      assert(sch != null)

      if (predTree != null) {

        val filterOperator= new 
        FilterOperator(sch,sch,predTree,null)              

        filterOperator.addParent(tscOperator)

        //"select *" case
        if(selList.size == 1 && 
          selList(0).isInstanceOf[STAR]){
          ret = filterOperator
        } else {

          if (grpbyList != null) {
            //get aggExp list from select list
            for(exp <-selList) {
              if (exp.isInstanceOf[AGG_EXPRESSION])
                aggExpList = aggExpList :+ exp 
            } 

            //create grpbyPreShuffleOperator 
            val gbyPreOp = new 
            GroupByPreShuffleOperator(sch,
              sch, gbyExpList,aggExpList)
            gbyPreOp.addParent(filterOperator)

            //create grpbyShuffleOperator
            val aggSch = sch.copy()
            val aggExpList2= aggExpList.filter(x=> x match {
                case a1: CNT_EXP=> false
                case _ => true 
              })
              
            var aggColList = aggExpList2.map(x=>x match{
                case a1: SUM_EXP=>a1.e.iden
                case a2: AVG_EXP=>a2.e.iden
                case a3: MIN_EXP=>a3.e.iden
                case a4: MAX_EXP=>a4.e.iden
                case _ => 
        throw new EdbException("not supported table type" )
              })
            val aggColIdx = aggColList.map(x=>
              Catalog.getAttIdx(x,sch))
            aggSch.setSchema(aggColIdx.toArray, sch)
            val gbyOp = new GroupByShuffleOperator(aggSch,sch,2, gbyExpList, aggExpList)
            gbyOp.addParent(gbyPreOp)
            ret = gbyOp
          } else{ 
            //grp by list is null

            //non STAR selection
            val projectOperator = new 
            ProjectOperator (sch,sch,selList)

            projectOperator.addParent(filterOperator)
            ret = projectOperator
          }

        } //end non select * and non empty predicate case
      } else { 
        //empty predicate, create project operator
        //"select *" case
        if(selList.size == 1 && 
          selList(0).isInstanceOf[STAR]){
          ret = tscOperator 
        } else {

          if (grpbyList != null) {
            //get aggExp list from select list
            for(exp <-selList) {
              if (exp.isInstanceOf[AGG_EXPRESSION])
                aggExpList = aggExpList :+ exp 
            } 

            //create grpbyPreShuffleOperator 
            val gbyPreOp = new 
            GroupByPreShuffleOperator(sch,
              sch, gbyExpList,aggExpList)
            gbyPreOp.addParent(tscOperator)

            //create grpbyShuffleOperator
            val aggSch = sch.copy()

            val aggExpList2= aggExpList.filter(x=> x match {
                case a1: CNT_EXP=> false
                case _ => true 
              })
            var aggColList = aggExpList2.map(x=>x match{
                case a1: SUM_EXP=>a1.e.iden
                case a2: AVG_EXP=>a2.e.iden
                case a3: MIN_EXP=>a3.e.iden
                case a4: MAX_EXP=>a4.e.iden
                case _ => 
        throw new EdbException("not supported agg type" )
              })
            aggColList= aggColList.filter(_!= None)
            val aggColIdx = aggColList.map(x=>
              Catalog.getAttIdx(x,sch))
            aggSch.setSchema(aggColIdx.toArray, sch)
            val gbyOp = new GroupByShuffleOperator(aggSch,sch,2, gbyExpList, aggExpList)
            gbyOp.addParent(gbyPreOp)
            ret = gbyOp
          }  else{

            val projectOperator = new 
            ProjectOperator (sch,sch,selList)
            projectOperator.addParent(tscOperator)

            ret = projectOperator
          }
        }
      }//end predicate is null

    }//end one table case
    ret
  } //end optimizeSelQB

}


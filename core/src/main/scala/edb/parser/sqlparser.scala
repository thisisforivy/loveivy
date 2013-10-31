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

/* AST Node */
sealed abstract class QUERYBLOCK

/* building block of AST tree */
sealed abstract class RELATION
case class TABLE(iden: String) extends RELATION
case class AS_TABLE(iden: String, alias: IDENTIFIER) extends RELATION

/* EXPRESSION is actually an EXPRESSION tree */
sealed abstract class EXPRESSION 
case class IDENTIFIER (iden: String) extends EXPRESSION
case class NUMBER(lit: Int) extends EXPRESSION
case class BOOLVAL(lit: Boolean) extends EXPRESSION
case class STRING(slit: String) extends EXPRESSION
case class STAR extends EXPRESSION
case class ADD(e1: EXPRESSION, e2: EXPRESSION) extends EXPRESSION
case class SUBTRACT(e1: EXPRESSION, e2: EXPRESSION) extends EXPRESSION 
case class MUL(e1: EXPRESSION, e2: EXPRESSION) extends EXPRESSION
case class DIV(e1: EXPRESSION, e2: EXPRESSION) extends EXPRESSION
case class AS_EXP(e: EXPRESSION, alias: IDENTIFIER) extends EXPRESSION

sealed abstract class AGG_EXPRESSION extends EXPRESSION
case class SUM_EXP(e: IDENTIFIER) extends AGG_EXPRESSION
case class AVG_EXP(e: IDENTIFIER) extends AGG_EXPRESSION
case class MIN_EXP(e: IDENTIFIER) extends AGG_EXPRESSION
case class MAX_EXP(e: IDENTIFIER) extends AGG_EXPRESSION
case class CNT_EXP(e: EXPRESSION) extends AGG_EXPRESSION

/* PREDICATE is actually an PREDICATE tree, with 2 operators, AND and  OR */
sealed abstract class PREDICATE 
case class PRED_AND(p1:PREDICATE, p2:PREDICATE) extends PREDICATE
case class PRED_OR(p1:PREDICATE, p2:PREDICATE) extends PREDICATE
case class PRED_EQUAL(e1: EXPRESSION, e2: EXPRESSION) extends PREDICATE 
case class PRED_NOTEQUAL(e1: EXPRESSION, e2: EXPRESSION) extends PREDICATE 
case class PRED_LESS(e1: EXPRESSION, e2: EXPRESSION) extends PREDICATE 
case class PRED_GREATER(e1: EXPRESSION, e2: EXPRESSION) extends PREDICATE 
case class PRED_LE(e1: EXPRESSION, e2: EXPRESSION) extends PREDICATE 
case class PRED_GE(e1: EXPRESSION, e2: EXPRESSION) extends PREDICATE 

/* main AST nodes: 4 lists; 2 binary AST trees */
case class SELECTLIST(sellist: List[EXPRESSION])
case class FROMLIST(fromlist: List[RELATION]) 
/* WHERE predicate tree */
case class GROUPBYLIST(grpbylist: List[EXPRESSION])
/* HAVING predicate tree */
case class ORDERBYLIST(ordbylist: List[EXPRESSION])

sealed abstract class COL_TYPE{
  def toString ():String
}
case class INT_TYPE  extends COL_TYPE{
  override def toString = "int"
}
case class FLOAT_TYPE extends COL_TYPE{
  override  def toString = "float"
}
case class DOUBLE_TYPE extends COL_TYPE{
  override  def toString = "double"
}
case class LONG_TYPE extends COL_TYPE{
  override  def toString = "long"
}
case class STRING_TYPE extends COL_TYPE{
  override  def toString = "string"
}
case class BOOL_TYPE extends COL_TYPE{
  override  def toString = "boolean"
}
case class DATE_TYPE extends COL_TYPE{
  override  def toString = "date"
}

case class TABLE_ELEMENT(name: String, coltype: COL_TYPE)

/* top level AST node */
case class XPLN_QB(
  qb: QUERYBLOCK
) extends QUERYBLOCK


case class SELECT_QB(
  sellist: SELECTLIST, 
  fromlist: FROMLIST,
  predtree: Option[PREDICATE], 
  grpbylist: Option[GROUPBYLIST], 
  havingtree: Option[PREDICATE],
  ordbylist: Option[ORDERBYLIST]
) extends QUERYBLOCK

case class SHOW_TABLE_QB  extends QUERYBLOCK
case class DESC_TABLE_QB(name: String)  extends QUERYBLOCK
case class CREATE_TABLE_QB(name: String, elementList: List[TABLE_ELEMENT]) extends QUERYBLOCK
case class DROP_TABLE_QB(name: String) extends QUERYBLOCK
/* populate table with random numbers */
case class GENERATE_TABLE_QB(name: String, count: Int) extends QUERYBLOCK



object SQLParser extends StandardTokenParsers {

  /* debugging tool e.g. def query = log(qb)("query") */
  override def log[T](p: => Parser[T])(name: String): Parser[T] = 
  Parser { 
    in => def prt(x: Input) = x.source.toString.drop(x.offset)
    println("trying " + name + " at " + prt(in))
    val r = p(in)
    println(name + " --> " + r + " next: " + prt(r.next))
    r
  }

  lexical.reserved += ("explain", "show", "drop", "create", "tables", "table",  "desc", "select", "from","as", "where", "or", "and", 
    "group", "by", "having", "order", "sum", "avg", "min", "max", 
    "count", "int", "float", "double", "long", "string",
    "boolean", "date", "generate", "true", "false")

  lexical.delimiters += ("+", "-","*","/", ",", "?", "(",")", 
    ".", "=", ">", "<", ">=", "<=", "<>")

  /* for unit test */
  def buildAST (input:String ):String = {

    /* tokenize the input stream */
    val tokens = new lexical.Scanner(input)

    /* start parsing the query, query is the root of EBNF
     * SQL syntax; tokens is a steam of token
     */
    val result = phrase(query)(tokens)

    /* when parsing succeed to a AST tree, call the interpreter over the AST tree */
    result match {
      case Success(tree, _) => tree.toString
      case e: NoSuccess => {
        Console.err.println(e)
        exit(100)
      }
    }
  }

  /**
    * Pure parsing. Generate the AST tree if 
    * syntax is correct
    */
  private def parse(input:String): QUERYBLOCK= {

    /* tokenize the input stream */
    val tokens = new lexical.Scanner(input)

    /* start parsing the query, query is the root of EBNF 
     * SQL syntax; tokens is a steam of token
     */
    val result = phrase(query)(tokens)

    result match {
      case Success(tree, _) => {
        //dump the AST tree
     /* println("***** AST tree starts ******")
        println(tree)
        println("***** AST tree ends ******")
        println()
        println()
        */

        tree 
      }
      case e: NoSuccess => {
        Console.err.println(e)
        exit(100)
      }
    }
  }

  /**
    * parser's external API (entrance point)
    *
    * Compile a input string into a physical plan 
    * a.k.a operator tree
    *
    *
    */
  def compile(input:String ): Operator[SequenceRecord] = {

    val qbAST: QUERYBLOCK = parse(input) 

    //this will return an optimized 
    //query physical plan
    qbAST match {
      //explain qb
      case e1: XPLN_QB => {
        Explain.explain(
          Optimizer.optimize(e1.qb),
          0)
        null
      }
      //sel qb
      case e2: SELECT_QB => Optimizer.optimize(qbAST)
      case e3: SHOW_TABLE_QB => {
        Catalog.showTables
        null
      }
      case e4: DESC_TABLE_QB =>{
        Catalog.descTable(e4.name) 
        null
      }
      case e5: CREATE_TABLE_QB =>{

        Catalog.createTable(e5)
        null
      }
      case e6: DROP_TABLE_QB =>{
        Catalog.dropTable(e6.name)
        null
      }
      case e7: GENERATE_TABLE_QB=>{
        Catalog.generateTable(e7.name, e7.count)
        null
      }
      case _ :  NoSuccess => {
        Console.err.println("not known qb")
        exit(100)
      }
    }
  }

  /* SQL EBNF production rules start */
  def query = qb|xplqb|ddl

  /* explain query */
  def xplqb: Parser[XPLN_QB] = "explain" ~> qb ^^ {e => new XPLN_QB(e)}

  def ddl: Parser[QUERYBLOCK]= show_table|desc_table|create_table|drop_table|generate_table

  def show_table: Parser[SHOW_TABLE_QB] = "show" ~ "tables" ^^^{new SHOW_TABLE_QB}

  def drop_table: Parser[DROP_TABLE_QB] = "drop" ~> "table" ~> ident ^^{ e => new DROP_TABLE_QB(e)}

  def generate_table: Parser[GENERATE_TABLE_QB] = "generate" ~ "table" ~ ident ~ digit^^{
    case _ ~ _ ~ name ~ cnt => new GENERATE_TABLE_QB(name, cnt.toInt)
  }

  def desc_table: Parser[DESC_TABLE_QB] =  "desc" ~> relation ^^{
    e => new DESC_TABLE_QB(e.iden)
  }
  //CREATE_TABLE_QB(name: String, elementList: List[TABLE_ELEMENT]) 
  def create_table: Parser[CREATE_TABLE_QB] = "create" ~ "table" ~ ident ~ 
  "(" ~ elelist ~ ")" ^^{
    case _ ~ _ ~ e ~ _ ~ collist ~ _  => new CREATE_TABLE_QB(e, collist)
  }

  def elelist: Parser[List[TABLE_ELEMENT]] = ele_sublist ~ (("," ~>ele_sublist)*) ^^
  {
    case e ~ l => e::l
  }

  def ele_sublist: Parser[TABLE_ELEMENT] = ident ~ ctype ^^{
    case e ~ l => TABLE_ELEMENT(e, l)
  }

  def ctype: Parser[COL_TYPE] = "int"^^^{new INT_TYPE } |
  "float"^^^{new FLOAT_TYPE} | "double"^^^{new DOUBLE_TYPE} |
  "long"^^^{new LONG_TYPE} | "string"^^^{new STRING_TYPE} |
  "boolean"^^^{new BOOL_TYPE} | "date"^^^{new DATE_TYPE}



  def qb: Parser[QUERYBLOCK] =( "select" ~ select_list ~ 
    "from" ~ from_list ~ (where_clause?) ~ (groupby_clause?) ~ (having_clause?) ~ 
    (ordby_clause?)^^
    {
      //0000
      case _ ~ sellist ~ _ ~ frolist ~ None ~ None ~ None ~ None
      => SELECT_QB(sellist, frolist, None,None, None, None)

      //0001
      case _ ~ sellist ~ _ ~ frolist ~ None ~ None ~ None ~ Some(ord)
      => SELECT_QB(sellist, frolist, None,None, None, Some(ord))

      //0010
      case _ ~ sellist ~ _ ~ frolist ~ None ~ None ~ Some(having) ~ None
      => SELECT_QB(sellist, frolist, None, None, Some(having), None)

      //0011
      case _ ~ sellist ~ _ ~ frolist ~ None ~ None ~ Some(having) ~ Some(ord)
      => SELECT_QB(sellist, frolist, None, None, Some(having), Some(ord))

      //0100
      case _ ~ sellist ~ _ ~ frolist ~ None ~ Some(grp) ~ None ~ None  
      => SELECT_QB(sellist, frolist, None, Some(grp), None, None)

      //0101
      case _ ~ sellist ~ _ ~ frolist ~ None ~ Some(grp) ~ None ~ Some(ord)  
      => SELECT_QB(sellist, frolist, None, Some(grp), None, Some(ord))

      //0110
      case _ ~ sellist ~ _ ~ frolist ~ None ~ Some(grp) ~ Some(having) ~ None 
      => SELECT_QB(sellist, frolist, None, Some(grp), Some(having), None)

      //0111
      case _ ~ sellist ~ _ ~ frolist ~ None ~ Some(grpby) ~ Some(having) ~ Some(ord)
      => SELECT_QB(sellist, frolist, None, Some(grpby), Some(having), Some(ord))

      //1000
      case _ ~ sellist ~ _ ~ frolist ~ Some(whr) ~ None ~ None ~ None  
      => SELECT_QB(sellist, frolist, Some(whr), None, None, None)

      //1001
      case _ ~ sellist ~ _ ~ frolist ~ Some(whr) ~ None ~ None~ Some(ord)  
      => SELECT_QB(sellist, frolist, Some(whr),None, None, Some(ord))

      //1010
      case _ ~ sellist ~ _ ~ frolist ~ Some(whr) ~ None ~ Some(having) ~ None
      => SELECT_QB(sellist, frolist, Some(whr), None, Some(having), None)

      //1011
      case _ ~ sellist ~ _ ~ frolist ~ Some(whr) ~ None ~ Some(having) ~ Some(ord)
      => SELECT_QB(sellist, frolist, Some(whr), None, Some(having),Some(ord))

      //1100
      case _ ~ sellist ~ _ ~ frolist ~ Some(whr) ~ Some(grpby) ~ None ~ None  
      => SELECT_QB(sellist, frolist, Some(whr), Some(grpby), None, None)

      //1101
      case _ ~ sellist ~ _ ~ frolist ~ Some(whr) ~ Some(grpby) ~ None ~ Some(ord) 
      => SELECT_QB(sellist, frolist, Some(whr), Some(grpby), None, Some(ord))

      //1110
      case _ ~ sellist ~ _ ~ frolist ~ Some(whr) ~ Some(grpby) ~ Some(having) ~ None
      => SELECT_QB(sellist, frolist, Some(whr), Some(grpby), Some(having), None)

      //1111
      case _ ~ sellist ~ _ ~ frolist ~ Some(whr) ~ Some(grpby) ~ Some(having) ~ Some(ord)
      => SELECT_QB(sellist, frolist, Some(whr), Some(grpby), Some(having), Some(ord))
    })

  def where_clause: Parser[PREDICATE] = "where" ~> pred_tree

  /* we need to add sum/avg/min/max/count expression for having clause */
  def having_clause: Parser[PREDICATE] = "having" ~> pred_tree

  def pred_tree: Parser[PREDICATE] = chainl1(bterm, "or" ^^^ PRED_OR)

  def bterm: Parser[PREDICATE] = chainl1(bfactor, "and" ^^^ PRED_AND)

  def bfactor: Parser[PREDICATE] = pred_primary | "("~>pred_tree<~")"

  def pred_primary: Parser[PREDICATE] = comp_pred

  def comp_pred: Parser[PREDICATE] = expression ~ ("="|"<>"|">"|">="|"<"|"<=") ~
  expression ^^
  {
    case e1 ~ "="  ~ e2 => PRED_EQUAL(e1,e2)
    case e1 ~ "<>" ~ e2 => PRED_NOTEQUAL(e1,e2)
    case e1 ~ ">"  ~ e2 => PRED_GREATER(e1,e2)
    case e1 ~ ">=" ~ e2 => PRED_GE(e1,e2)
    case e1 ~ "<"  ~ e2 => PRED_LESS(e1,e2)
    case e1 ~ "<=" ~ e2 => PRED_LE(e1,e2)
  }

  def groupby_clause: Parser[GROUPBYLIST]= "group" ~ "by" ~ grpby_sublist ~ 
  (("," ~> grpby_sublist)*)^^
  {
    case _ ~ _ ~ e ~ l => GROUPBYLIST(e::l)
  }

  def grpby_sublist: Parser[EXPRESSION]= (expression ~ (as_clause?))^^
  { 
    case e ~ None => e
    case e ~ Some(as) => AS_EXP(e,as)
  }

  def ordby_clause: Parser[ORDERBYLIST] = "order" ~ "by" ~ 
  ordby_sublist ~(("," ~> ordby_sublist)*)^^
  {
    case _ ~ _ ~ e ~ l => ORDERBYLIST(e::l)
  }

  def ordby_sublist: Parser[EXPRESSION]= (expression ~ (as_clause?))^^
  { 
    case e ~ None => e
    case e ~ Some(as) => AS_EXP(e,as)
  }

  def from_list: Parser[FROMLIST] = table_ref_list ~ (("," ~> table_ref_list)*) ^^
  {
    case r1 ~ r2 => FROMLIST(r1::r2)
  }

  def table_ref_list: Parser[RELATION] = relation~(as_clause?) ^^ 
  {
    case r ~ None => r
    case r ~ Some(as) => AS_TABLE(r.iden,as)
  }

  def relation: Parser[TABLE] = (ident~(("."~>ident)?)^^{ 
      case r ~ None => TABLE(r) 
      case r ~ Some(r2) => TABLE(r+ "." + r2)
    })


  def select_list: Parser[SELECTLIST] = ("*" ^^^{SELECTLIST(STAR()::Nil)}|
    select_sublist ~ (("," ~> select_sublist)*)^^ 
    {
      case e ~ l => SELECTLIST(e::l)
    })|"count" ~> "(" ~> "*" <~ ")" ^^^{SELECTLIST(CNT_EXP(IDENTIFIER("*"))::Nil)}

  def select_sublist:Parser[EXPRESSION] = ((expression~(as_clause?)) ^^
    { 
      case e ~ None => e
      case e ~ Some(as) => AS_EXP(e,as)
    })

  def as_clause: Parser[IDENTIFIER] = "as"~>alias|alias

  def alias: Parser[IDENTIFIER] = ident^^{s => IDENTIFIER(s)}

  //def expression:Parser[EXPRESSION] = term
  def expression: Parser[EXPRESSION] = chainl1(term, "+" ^^^ADD|"-" ^^^SUBTRACT)

  /* chainl1 is chain left at least 1 */
  def term = chainl1(factor, "*"^^^MUL|"/"^^^DIV)

  def factor =  numeric_primary | "("~>expression<~")" 

  /* NUMERIC_PRIMARY ::= NUMERIC_LITERAL|COLUMN| '(' EXPRESSION ')' */
  def numeric_primary: Parser[EXPRESSION] = agg|string_literal|numeric_literal|column|boolval
  "("~>expression<~")"

  def agg: Parser[EXPRESSION] =  "sum" ~ "(" ~ column ~ ")" ^^ 
  {
    case _ ~ _ ~ col ~ _ => SUM_EXP(col)
  }| "avg" ~ "(" ~ column ~ ")" ^^ 
  {
    case _ ~ _ ~ col ~ _ => AVG_EXP(col)
  }|"min" ~ "(" ~ column ~ ")" ^^ 
  {
    case _ ~ _ ~ col ~ _ => MIN_EXP(col)
  }|"max" ~ "(" ~ column ~ ")" ^^ 
  {
    case _ ~ _ ~ col ~ _ => MAX_EXP(col)
  }|"count" ~> "(" ~> cnt_exp <~ ")" 

  def cnt_exp: Parser[EXPRESSION] = "*" ^^^ {CNT_EXP(IDENTIFIER("*"))}|
column ^^ {c =>CNT_EXP(c)}| numeric_literal ^^ {c => CNT_EXP(c)}

/* STRING_LITERAL ::= STRING */
def string_literal:Parser[EXPRESSION] =stringLit^^{s => STRING(s.toString)}

def str:Parser[String] = stringLit

/* NUMERIC_LITERAL ::= DIGIT */
def numeric_literal:Parser[EXPRESSION] = digit^^{s => NUMBER(s.toInt)}

def digit:Parser[String] = numericLit

/* COLUMN ::= IDENTIFIER */
def column:Parser[IDENTIFIER] = ident^^{e => IDENTIFIER(e)}

def boolval:Parser[BOOLVAL] = "true"^^^{new BOOLVAL(true)}|
"false"^^^{new BOOLVAL(false)}

}


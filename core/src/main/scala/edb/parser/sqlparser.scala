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
case class STRING(slit: String) extends EXPRESSION
case class STAR extends EXPRESSION
case class ADD(e1: EXPRESSION, e2: EXPRESSION) extends EXPRESSION
case class SUBTRACT(e1: EXPRESSION, e2: EXPRESSION) extends EXPRESSION 
case class MUL(e1: EXPRESSION, e2: EXPRESSION) extends EXPRESSION
case class DIV(e1: EXPRESSION, e2: EXPRESSION) extends EXPRESSION
case class AS_EXP(e: EXPRESSION, alias: IDENTIFIER) extends EXPRESSION
case class SUM_EXP(e: IDENTIFIER) extends EXPRESSION
case class AVG_EXP(e: IDENTIFIER) extends EXPRESSION
case class MIN_EXP(e: IDENTIFIER) extends EXPRESSION
case class MAX_EXP(e: IDENTIFIER) extends EXPRESSION
case class CNT_EXP(e: IDENTIFIER) extends EXPRESSION

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

/* top level AST node */
case class SELECT_QB(
  sellist: SELECTLIST, 
  fromlist: FROMLIST,
  predtree: Option[PREDICATE], 
  grpbylist: Option[GROUPBYLIST], 
  havingtree: Option[PREDICATE],
  ordbylist: Option[ORDERBYLIST]
) extends QUERYBLOCK

case class XPLN_QB(
  qb: QUERYBLOCK
) extends QUERYBLOCK



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

  lexical.reserved += ("explain", "select", "from","as", "where", "or", "and", 
    "group", "by", "having", "order", "sum", "avg", "min", "max", 
    "count")

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
        println("***** AST tree starts ******")
        println(tree)
        println("***** AST tree ends ******")
        println()
        println()
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
      case _ :  NoSuccess => {
        Console.err.println("not known qb")
        exit(100)
      }
    }
  }

  /* SQL EBNF production rules start */
  def query = qb|xplqb

  /* explain query */
  def xplqb: Parser[XPLN_QB] = "explain" ~> qb ^^ {e => new XPLN_QB(e)}


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

  def relation: Parser[TABLE] = (ident~(("."~ident)?)^^{ 
      case r ~ None => TABLE(r) 
      case r ~ Some(r2) => TABLE(r+ r2)
    })


  def select_list: Parser[SELECTLIST] = ("*" ^^^{SELECTLIST(STAR()::Nil)}|
    select_sublist ~ (("," ~> select_sublist)*)^^ 
    {
      case e ~ l => SELECTLIST(e::l)
    })

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
  def numeric_primary: Parser[EXPRESSION] = agg|string_literal|numeric_literal|column
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
column ^^ {c =>CNT_EXP(c)}

/* STRING_LITERAL ::= STRING */
def string_literal:Parser[EXPRESSION] =stringLit^^{s => STRING(s.toString)}

def str:Parser[String] = stringLit

/* NUMERIC_LITERAL ::= DIGIT */
def numeric_literal:Parser[EXPRESSION] = digit^^{s => NUMBER(s.toInt)}

def digit:Parser[String] = numericLit

/* COLUMN ::= IDENTIFIER */
def column:Parser[IDENTIFIER] = ident^^{e => IDENTIFIER(e)}

}


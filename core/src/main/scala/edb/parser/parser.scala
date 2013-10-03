/* find the class you want to use in www.scala-lang.org/api, at the top  you will see
 the package name , append ._ at the end of the package name. Then, you will be able to 
 use the class
 */
import scala.util.parsing.combinator.syntactical._
import scala.io._

sealed abstract class Statement

case class Print(expr: Expr) extends Statement
case class Space extends Statement

case class Repeat(times: Int, stmts: List[Statement]) extends Statement
case class Let(val id: String, val expr: Expr) extends Statement

sealed abstract class Expr {
  def value(context: Context): String
}

case class Literal(text: String) extends Expr {
  override def value(context: Context) = text
}

case class Variable(id: String) extends Expr {
  override def value(context: Context) = {
    context.resolve(id) match {
      case Some(binding) => binding.expr.value(context)
      case None => throw new RuntimeException("Unknown identifier: " + id)
    }
  }
}

case class Hello extends Expr {
  override def value(context: Context) = "Hello, World!"
}

case class Goodbye extends Expr {
  override def value(context: Context) = "Farewell, sweet petunia!"
}

class Context(ids: Map[String, Let], parent: Option[Context]) {
  //when visit other AST nodes, sometime you need to clone context to be used
  //by others
  lazy val child = new Context(Map[String, Let](), Some(this))

  // add new id->Let statement mapping
  def +(binding: Let) = {
    val newIDs = ids + (binding.id -> binding)
    new Context(newIDs, parent)
  }

  //given an identifier, return the let statement
  def resolve(id: String): Option[Let] = {
    if (ids contains id) {
      Some(ids(id))
    } else {
      parent match {
        case Some(c) => c resolve id
        case None => None
      }
    }
  }
}

object EmptyContext extends Context(Map[String, Let](), None)

class Interpreter1(tree: List[Statement]) {
  def run() {
    walkTree(tree, EmptyContext)
  }

  //when visiting AST tree, a list(a sequence) of statements, we take corresponding actions
  //for each statement (AST nodes)
  private def walkTree(tree: List[Statement], context: Context) {
    tree match {
      case Print(expr) :: rest => {
        println(expr.value(context))
        walkTree(rest, context)
      }

      case Space() :: rest => {
        println()
        walkTree(rest, context)
      }

      case Repeat(times, stmts) :: rest => {
        for (i <- 0 until times) {
          walkTree(stmts, context.child)
        }

        walkTree(rest, context)
      }

      case (binding: Let) :: rest => walkTree(rest, context + binding)

      case Nil => ()
    }
  }
}

object Simpletalk extends StandardTokenParsers {

  lexical.reserved += ("print", "space", "repeat", "next", "let", "HELLO", "GOODBYE")
  lexical.delimiters += ("=")

  val input = Source.fromFile("/Users/mwu/input.talk").getLines.reduceLeft[String](_ + '\n' + _)
  val tokens = new lexical.Scanner(input)

  val result = phrase(program)(tokens)

  result match {
    case Success(tree, _) => new Interpreter1(tree).run()

    case e: NoSuccess => {
      Console.err.println(e)
      exit(100)
    }
  }
  // grammar starts here
  def program = stmt+

  //expr below is a non-terminal, it's production follows stmt
  def stmt: Parser[Statement] = ( "print" ~ expr ^^ { case _ ~ e => Print(e) }
    | "space" ^^^ Space()
    | "repeat" ~ numericLit ~ (stmt+) ~ "next" ^^ {
      case _ ~ times ~ stmts ~ _ => Repeat(times.toInt, stmts)
    } 
    | "let" ~ ident ~ "=" ~ expr ^^ { 
      case _ ~ id ~ _ ~ e => Let(id, e) 
    } )

  def expr = ( "HELLO" ^^^ Hello()
    | "GOODBYE" ^^^ Goodbye()
    | stringLit ^^ { case s => Literal(s) }
    | numericLit ^^ { case s => Literal(s) }
    | ident ^^ { case id => Variable(id) } )

}

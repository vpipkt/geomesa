package org.locationtech.geomesa.convert.common

import scala.collection.mutable
import scala.util.parsing.combinator.JavaTokenParsers

trait Transformers extends JavaTokenParsers {

  lazy val functionMap = mutable.HashMap[String, TransformerFnFactory]()

  def registerFunctionFactory(functionFactory: TransformerFnFactory): Unit = {
    functionFactory.functions.foreach { f => functionMap.put(f, functionFactory) }
  }

  trait TransformerParser {
    private val OPEN_PAREN  = "("
    private val CLOSE_PAREN = ")"

    def string      = "'" ~> "[^']+".r <~ "'".r ^^ { s => LitString(s) }
    def int         = wholeNumber   ^^ { i => LitInt(i.toInt) }
    def double      = decimalNumber ^^ { d => LitDouble(d.toDouble) }
    def lit         = string | double | int
    def wholeRecord = "$0" ^^ { _ => WholeRecord }
    def column      = "$" ~> "[1-9][0-9]*".r ^^ { i => Col(i.toInt) }
    def fnName      = ident ^^ { n => LitString(n) }
    def fn          = (fnName <~ OPEN_PAREN) ~ (rep1sep(expr, ",") <~ CLOSE_PAREN) ^^ {
      case LitString(name) ~ e => FunctionExpr(functionMap(name).build(name, e: _*))
    }
    def expr: Parser[Expr] = fn | wholeRecord | column | lit
  }

  lazy val transformerParser = new TransformerParser {}

  trait TransformerFn extends TransformerParser {
    def eval(args: Any*): Any
  }

  sealed trait Expr {
    def eval(args: Any*): Any
  }

  sealed trait Lit[T <: Any] extends Expr {
    def value: T
    override def eval(args: Any*): Any = value
  }

  case class LitString(value: String) extends Lit[String]
  case class LitInt(value: Integer) extends Lit[Integer]
  case class LitDouble(value: java.lang.Double) extends Lit[java.lang.Double]
  case object WholeRecord extends Expr {
    override def eval(args: Any*): Any = args(0)
  }
  case class Col(i: Int) extends Expr {
    override def eval(args: Any*): Any = args(i-1)
  }
  case class FunctionExpr(f: TransformerFn) extends Expr {
    override def eval(args: Any*): Any = f.eval(args: _*)
  }

  trait TransformerFnFactory extends TransformerParser {
    def functions: Seq[String]
    def build(name: String, args: Expr*): TransformerFn
  }

  object StringFnFactory extends TransformerFnFactory {

    override def functions: Seq[String] = Seq("trim", "capitalize", "lowercase", "regexReplace")

    def build(name: String, expressions: Expr*) = name match {
      case "trim" =>
        val exp = expressions.head
        val fn: String => String = { s =>
          s.trim
        }
        StringFn(fn, exp)

      case "capitalize" =>
        val exp = expressions.head
        val fn: String => String = _.capitalize
        StringFn(fn, exp)

      case "lowercase" =>
        val exp = expressions.head
        val fn: String => String = _.toLowerCase
        StringFn(fn, exp)

      case "regexReplace" =>
        val Seq(LitString(regex), LitString(replacement), e) = expressions
        val compiledRegex = regex.r
        val fn: String => String = arg => {
          compiledRegex.replaceAllIn(arg, replacement)
        }
        StringFn(fn, e)

      case e =>
        println(e)
        null
    }

    case class StringFn(f: String => String, exp: Expr) extends TransformerFn {
      override def eval(args: Any*): Any = f(exp.eval(args: _*).asInstanceOf[String])
    }
  }

  registerFunctionFactory(StringFnFactory)

  def parse(s: String): Expr = parse(transformerParser.expr, s).get

}

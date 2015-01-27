package org.locationtech.geomesa.convert

import java.nio.charset.StandardCharsets
import java.util.UUID
import javax.imageio.spi.ServiceRegistry

import com.google.common.hash.Hashing
import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.codec.binary.Base64
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.parsing.combinator.JavaTokenParsers

object Transformers extends JavaTokenParsers {

  val functionMap = mutable.HashMap[String, TransformerFunctionFactory]()
  ServiceRegistry.lookupProviders(classOf[TransformerFunctionFactory]).foreach { factory =>
    factory.functions.foreach { f => functionMap.put(f, factory) }
  }

  object TransformerParser {
    private val OPEN_PAREN  = "("
    private val CLOSE_PAREN = ")"

    def string      = "'" ~> "[^']+".r <~ "'".r ^^ { s => LitString(s) }
    def int         = wholeNumber ^^ { i => LitInt(i.toInt) }
    def double      = decimalNumber ^^ { d => LitDouble(d.toDouble) }
    def lit         = string | double | int
    def wholeRecord = "$0" ^^ { _ => WholeRecord }
    def regexExpr   = string <~ "::r" ^^ { case LitString(s) => RegexExpr(s) }
    def column      = "$" ~> "[1-9][0-9]*".r ^^ { i => Col(i.toInt) }
    def cast2int    = expr <~ "::int" ^^ { e => Cast2Int(e) }
    def cast2double = expr <~ "::double" ^^ { e => Cast2Double(e) }
    def fieldLookup = "$" ~> ident ^^ { i => FieldLookup(i) }
    def fnName      = ident ^^ { n => LitString(n) }
    def fn          = (fnName <~ OPEN_PAREN) ~ (repsep(transformExpr, ",") <~ CLOSE_PAREN) ^^ {
      case LitString(name) ~ e => FunctionExpr(functionMap(name).build(name), e)
    }
    def expr = fn | wholeRecord | regexExpr | fieldLookup | column | lit
    def transformExpr: Parser[Expr] = cast2double | cast2int | expr
  }

  class EvaluationContext(fieldNameMap: Map[String, Int], var computedFields: Array[Any]) {
    def indexOf(n: String) = fieldNameMap(n)
    def lookup(i: Int) = computedFields(i)
  }

  sealed trait Expr {
    def eval(args: Any*)(implicit ctx: EvaluationContext): Any
  }

  sealed trait Lit[T <: Any] extends Expr {
    def value: T
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = value
  }

  case class LitString(value: String) extends Lit[String]
  case class LitInt(value: Integer) extends Lit[Integer]
  case class LitDouble(value: java.lang.Double) extends Lit[java.lang.Double]
  case class Cast2Int(e: Expr) extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = e.eval(args: _*).asInstanceOf[String].toInt
  }
  case class Cast2Double(e: Expr) extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = e.eval(args: _*).asInstanceOf[String].toDouble
  }
  case object WholeRecord extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = args(0)
  }
  case class Col(i: Int) extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = args(i)
  }
  case class FieldLookup(n: String) extends Expr {
    var idx = -1
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = {
      if(idx == -1) idx = ctx.indexOf(n)
      ctx.lookup(idx)
    }
  }
  case class RegexExpr(s: String) extends Expr {
    val compiled = s.r
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = compiled
  }
  case class FunctionExpr(f: TransformerFn, arguments: Seq[Expr]) extends Expr {
    override def eval(args: Any*)(implicit ctx: EvaluationContext): Any = f.eval(arguments.map(_.eval(args: _*)): _*)
  }

  def parse(s: String): Expr = parse(TransformerParser.transformExpr, s).get

}

trait TransformerFn {
  def eval(args: Any*): Any
}

trait TransformerFunctionFactory {
  def functions: Seq[String]
  def build(name: String): TransformerFn
}

class StringFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[String] = Seq("trim", "capitalize", "lowercase", "regexReplace", "concat")

  def build(name: String) = name match {
    case "trim" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String].trim
      StringFn(f)

    case "capitalize" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String].capitalize
      StringFn(f)

    case "lowercase" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String].toLowerCase
      StringFn(f)

    case "regexReplace" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[Regex].replaceAllIn(args(2).asInstanceOf[String], args(1).asInstanceOf[String])
      StringFn(f)

    case "concat" =>
      val f: (Any*) => Any = args => args(0).asInstanceOf[String] + args(1).asInstanceOf[String]
      StringFn(f)

    case e =>
      println(e)
      null
  }

  case class StringFn(f: (Any*) => Any) extends TransformerFn {
    override def eval(args: Any*): Any = f(args: _*)
  }
}

class DateFunctionFactory extends TransformerFunctionFactory {
  override def functions: Seq[String] = Seq("date", "isodate", "isodatetime", "dateHourMinuteSecondMillis")

  override def build(name: String): TransformerFn = name match {
    case "date"                        => new CustomFormatDateParser
    case "isodate"                     => StandardDateParser(ISODateTimeFormat.basicDate())
    case "isodatetime"                 => StandardDateParser(ISODateTimeFormat.basicDateTime())
    case "dateHourMinuteSecondMillis"  => StandardDateParser(ISODateTimeFormat.dateHourMinuteSecondMillis())
  }

  case class StandardDateParser(format: DateTimeFormatter) extends TransformerFn {
    override def eval(args: Any*): Any = format.parseDateTime(args(0).toString).toDate
  }

  class CustomFormatDateParser(var format: DateTimeFormatter = null) extends TransformerFn {
    override def eval(args: Any*): Any = {
      if(format == null) format = DateTimeFormat.forPattern(args(0).asInstanceOf[String])
      format.parseDateTime(args(1).asInstanceOf[String]).toDate
    }
  }
}

class GeometryFunctionFactory extends TransformerFunctionFactory {
  override def functions: Seq[String] = Seq("point")

  override def build(name: String): TransformerFn = name match {
    case "point" => PointParserFn
  }
  
  case object PointParserFn extends TransformerFn {
    val gf = JTSFactoryFinder.getGeometryFactory
    override def eval(args: Any*): Any = 
      gf.createPoint(new Coordinate(args(0).asInstanceOf[Double], args(1).asInstanceOf[Double]))
  }
  
}

class IdFunctionFactory extends TransformerFunctionFactory {
  override def functions: Seq[String] = Seq("md5", "uuid", "base64", "string2bytes")

  override def build(name: String): TransformerFn = name match {
    case "md5"          => MD5()
    case "uuid"         => UUIDFn()
    case "base64"       => Base64Encode()
    case "string2bytes" => String2Bytes()
  }

  case class String2Bytes() extends TransformerFn {
    override def eval(args: Any*): Any = args(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
  }

  case class MD5() extends TransformerFn {
    val hasher = Hashing.md5()
    override def eval(args: Any*): Any = hasher.hashBytes(args(0).asInstanceOf[Array[Byte]]).toString
  }

  case class UUIDFn() extends TransformerFn {
    override def eval(args: Any*): Any = UUID.randomUUID().toString
  }

  case class Base64Encode() extends TransformerFn {
    override def eval(args: Any*): Any = Base64.encodeBase64URLSafeString(args(0).asInstanceOf[Array[Byte]])
  }
}
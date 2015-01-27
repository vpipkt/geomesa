package org.locationtech.geomesa.convert.avro

import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.convert.common.Transformers

trait AvroTransformers extends Transformers {

  object AvroPathFnFactory extends TransformerFnFactory {

    override def functions: Seq[String] = Seq("avroPath")

    override def build(name: String, args: Expr*): TransformerFn = args match {
      case Seq(expression, LitString(path)) => AvroPathFn(path, expression)
      case _ => throw new IllegalArgumentException()
    }

    case class AvroPathFn(path: String, col: Expr) extends TransformerFn {
      val apath = AvroPath(path)
      override def eval(args: Any*): Any = apath.eval(col.eval(args: _*).asInstanceOf[GenericRecord]).orNull
    }
  }

  registerFunctionFactory(AvroPathFnFactory)
}

package org.locationtech.geomesa.convert


import com.typesafe.config.{Config, ConfigFactory}
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr, Predicate}
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.Try

case class Field(name: String, transform: Transformers.Expr)

trait Converters[I, T] {

  def buildConverter(conf: Config): ToSimpleFeatureConverter[I, T]

  def buildFields(fields: Seq[Config]): IndexedSeq[Field] =
    fields.map { f =>
      val name = f.getString("name")
      val transform = Transformers.parseTransform(f.getString("transform"))
      Field(name, transform)
    }.toIndexedSeq

  def buildIdBuilder(t: String) = Transformers.parseTransform(t)

  def findTargetSFT(name: String): SimpleFeatureType = {
    val fileName = s"sft_${name}.conf"
    val conf = ConfigFactory.load(fileName)
    buildSpec(conf)
  }

  def buildCompositeConverter(origConf: Config): CompositeConverter[I, T] = {
    val conf = origConf.getConfig("composite-converter")
    val converters: Seq[(Predicate, ToSimpleFeatureConverter[I, T])] =
      conf.getConfigList("converters").map { c =>
        val pred = Transformers.parsePred(c.getString("predicate"))
        val converter = buildConverter(conf.getConfig(c.getString("converter")))
        (pred, converter)
      }
    new CompositeConverter[I, T](findTargetSFT(conf.getString("type-name")), converters)
  }

  def buildSpec(c: Config) = {
    val typeName         = c.getString("type-name")
    val fields           = c.getConfigList("attributes")

    val fieldSpecs = fields.map { case field =>
      val name    = field.getString("name")
      val default = Try { field.getBoolean("default") }.map(_ => "*").getOrElse("")
      val typ     = field.getString("type")
      val srid    = if (field.hasPath("srid")) s":srid=${field.getString("srid")}" else ""
      val index   = if (field.hasPath("index")) s":index=${field.getBoolean("index")}" else ""
      val stIdx   = if (field.hasPath("index-value")) s":index-value=${field.getBoolean("index-value")}" else ""
      s"$default$name:$typ$srid$index$stIdx"
    }

    val spec = fieldSpecs.mkString(",")
    val sft = SimpleFeatureTypes.createType(typeName, spec)
    sft
  }

}

trait SimpleFeatureConverter[I, T] {
  def targetSFT: SimpleFeatureType
  def processInput(is: Iterator[I]): Iterator[SimpleFeature]
}

trait ToSimpleFeatureConverter[I, T] extends SimpleFeatureConverter[I, T] {
  def targetSFT: SimpleFeatureType
  def inputFields: IndexedSeq[Field]
  def idBuilder: Expr
  def fromInputType(i: I): T
  val fieldNameMap = inputFields.zipWithIndex.map { case (f, idx) => (f.name, idx)}.toMap

  val featureFactory = new AvroSimpleFeatureFactory
  implicit val ctx = new EvaluationContext(fieldNameMap, null)
  def convert(t: T, reuse: Array[Any], sfAttrReuse: Array[Any]): SimpleFeature = {
    val attributes =
      if(reuse == null) Array.ofDim[Any](inputFields.length)
      else reuse
    ctx.computedFields = attributes

    inputFields.zipWithIndex.foreach { case (field, i) =>
      attributes(i) = applyTransform(field.transform, t)
    }

    val sfAttributes =
      if(sfAttrReuse == null) Array.ofDim[Any](targetSFT.getAttributeCount)
      else sfAttrReuse

    indexes.foreach { case (targetIndex: Int, inputIndex: Int) =>
        sfAttributes(targetIndex) = attributes(inputIndex)
    }

    val id = applyTransform(idBuilder, t).asInstanceOf[String]
    featureFactory.createSimpleFeature(sfAttributes.asInstanceOf[Array[AnyRef]], targetSFT, id)
  }

  def applyTransform(fn: Expr, t: T): Any
  def applyPredicate(pred: Predicate, t: T): Boolean

  val reuse = Array.ofDim[Any](inputFields.length)
  val sfAttrReuse = Array.ofDim[Any](targetSFT.getAttributeCount)

  def processSingleInput(i: I): SimpleFeature =
    convert(fromInputType(i), reuse, sfAttrReuse)

  def processInput(is: Iterator[I]): Iterator[SimpleFeature] =
    is.map { s => processSingleInput(s) }

  val indexes =
    targetSFT.getAttributeDescriptors.flatMap { attr =>
      val targetIdx = targetSFT.indexOf(attr.getName)
      val attrName  = attr.getLocalName
      val inputIdx  = inputFields.indexWhere { f => f.name.equals(attrName) }

      if(inputIdx == -1) None
      else               Some((targetIdx, inputIdx))

    }

}

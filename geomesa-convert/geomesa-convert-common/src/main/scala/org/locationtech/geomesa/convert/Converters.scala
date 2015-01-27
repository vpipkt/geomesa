package org.locationtech.geomesa.convert


import com.typesafe.config.{Config, ConfigFactory}
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr}
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.Try

case class Field(name: String, transform: Transformers.Expr)

trait Converters {

  def buildFields(fields: Seq[Config]): IndexedSeq[Field] =
    fields.map { f =>
      val name = f.getString("name")
      val transform = Transformers.parse(f.getString("transform"))
      Field(name, transform)
    }.toIndexedSeq

  def buildIdBuilder(t: String) = Transformers.parse(t)

  def findTargetSFT(name: String): SimpleFeatureType = {
    val fileName = s"sft_${name}.conf"
    val conf = ConfigFactory.load(fileName)
    buildSpec(conf)
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

trait ToSimpleFeatureConverter[I, T] {
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

  def processInput(is: Iterator[I]): Iterator[SimpleFeature] = {
    val reuse = Array.ofDim[Any](inputFields.length)
    val sfReuse = Array.ofDim[Any](targetSFT.getAttributeCount)
    is.map { s =>
      convert(fromInputType(s), reuse, sfReuse)
    }
  }

  val indexes =
    targetSFT.getAttributeDescriptors.flatMap { attr =>
      val targetIdx = targetSFT.indexOf(attr.getName)
      val attrName  = attr.getLocalName
      val inputIdx  = inputFields.indexWhere { f => f.name.equals(attrName) }

      if(inputIdx == -1) None
      else               Some((targetIdx, inputIdx))

    }

}

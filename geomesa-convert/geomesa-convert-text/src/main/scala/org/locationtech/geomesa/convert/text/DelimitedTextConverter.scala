package org.locationtech.geomesa.convert.text

import com.google.common.base.Splitter
import com.google.common.collect.ObjectArrays
import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert.{Converters, Field, ToSimpleFeatureConverter}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

object DelimitedTextConverterBuilder extends Converters {
  def apply(conf: Config): DelimitedTextConverter = {
    val delimiter = conf.getString("delimiter")
    val fields    = buildFields(conf.getConfigList("fields"))
    val targetSFT = findTargetSFT(conf.getString("type-name"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))
    new DelimitedTextConverter(delimiter, targetSFT, idBuilder, fields)
  }
}

class DelimitedTextConverter(delimiter: String, val targetSFT: SimpleFeatureType, val idBuilder: Expr, val inputFields: IndexedSeq[Field])
  extends ToSimpleFeatureConverter[String, Array[String]] {

  val splitter = Splitter.on(delimiter)

  override def fromInputType(string: String): Array[String] = {
    val splitIter = splitter.split(string).toArray
    ObjectArrays.concat(string, splitIter)
  }

  override def applyTransform(fn: Expr, t: Array[String]): Any = fn.eval(t: _*)
}

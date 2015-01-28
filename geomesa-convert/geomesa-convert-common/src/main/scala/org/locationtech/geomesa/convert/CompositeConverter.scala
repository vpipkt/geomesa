package org.locationtech.geomesa.convert

import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Predicate}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class CompositeConverter[I, T](val targetSFT: SimpleFeatureType,
                               converters: Seq[(Predicate, ToSimpleFeatureConverter[I, T])])
  extends SimpleFeatureConverter[I, T] {

  // to maintain laziness
  val convView = converters.view

  override def processInput(is: Iterator[I]): Iterator[SimpleFeature] =
    is.map { input =>
      convView.flatMap { case (pred, conv) =>  processIfValid(input, pred, conv) }.head
    }

  implicit val ec = new EvaluationContext(Map(), Array())
  def processIfValid(input: I, pred: Predicate, conv: ToSimpleFeatureConverter[I, T]) =
    if(pred.eval(input)) Some(conv.processSingleInput(input)) else None
}




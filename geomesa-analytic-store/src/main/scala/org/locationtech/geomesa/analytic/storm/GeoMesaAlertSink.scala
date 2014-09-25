package org.locationtech.geomesa.analytic.storm

import java.util.{Date, UUID}

import backtype.storm.tuple.Tuple
import org.locationtech.geomesa.analytic.storm.GeoMesaAlertSinkParams._
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Write simple features to GeoMesa...input is triples "alertID", "site", "time"
 */
class GeoMesaAlertSink extends GeoMesaSink {

  override def buildFeature(input: Tuple) = {
    val alert = input.getValue(0).asInstanceOf[UUID].toString
    val site = input.getString(1)
    val time = input.getValue(2).asInstanceOf[Date]
    val geom = WKTUtils.read("POINT(45 45)")
    AvroSimpleFeatureFactory.buildAvroFeature(getSft, List(alert, site, time, geom), alert)
  }

  override def parseSft(stormConf: java.util.Map[_, _]): SimpleFeatureType = {
    def getStr(param: String) = stormConf.get(param).asInstanceOf[String]
    SimpleFeatureTypes.createType(getStr(SftName), getStr(Sft))
  }

  override def getSinkName: String = "Alert"
}

object GeoMesaAlertSinkParams {
  val PRE        = "geomesa.sink.alert"
  val SftName    = s"$PRE.sftName"
  val Sft        = s"$PRE.sft"
}


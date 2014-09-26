package org.locationtech.geomesa.analytic.storm

import java.util.Date

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
    val alertId = input.getString(0)
    val site = input.getString(1)
    val time = input.getValue(2).asInstanceOf[Date]
    val geom = getSiteGeom(site)
    AvroSimpleFeatureFactory.buildAvroFeature(getSft, List(alertId, site, time, geom), alertId)
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


package org.locationtech.geomesa.analytic.storm

import java.util.{Date, UUID}

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import org.geotools.data.{DataStoreFinder, FeatureStore}
import org.geotools.feature.DefaultFeatureCollection
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.analytic.storm.GeoMesaSinkParams._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._

/**
 * Write time series to GeoMesa...input is "date", "site", "count", "alert", "alertId"
 */
class GeoMesaTimeSeriesSink extends GeoMesaSink {

  override def buildFeature(input: Tuple) = {
    val time = input.getValue(0).asInstanceOf[Date]
    val site = input.getString(1)
    val count = input.getInteger(2)
    val isAlert = input.getBoolean(3)
    val alertId = input.getString(4)
    val geom = getSiteGeom(site)

    // site:String:index=true,count:Int:index=true,isAlert:Boolean:index=true,dtg:Date,*geom:Point
    AvroSimpleFeatureFactory.buildAvroFeature(getSft, List(site, count, isAlert, alertId, time, geom), UUID.randomUUID().toString)
  }

  override def parseSft(stormConf: java.util.Map[_, _]): SimpleFeatureType = {
    import GeoMesaTimeSeriesSink._
    def getStr(param: String) = stormConf.get(param).asInstanceOf[String]
    SimpleFeatureTypes.createType(getStr(SftName), getStr(Sft))
  }

  override def getSinkName: String = "TimeSeries"

}

object GeoMesaTimeSeriesSink {
  val PRE        = "geomesa.sink.timeseries"
  val SftName    = s"$PRE.sftName"
  val Sft        = s"$PRE.sft"
}




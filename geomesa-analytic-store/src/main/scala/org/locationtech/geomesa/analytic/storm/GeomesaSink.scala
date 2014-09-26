package org.locationtech.geomesa.analytic.storm

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import org.geotools.data.{DataUtilities, DataStoreFinder, FeatureStore}
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.core.data._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._

/**
 * Abstract GeoMesa Sink
 */
abstract class GeoMesaSink extends BaseRichBolt {
  private var outputCollector: OutputCollector = null
  private var sft: SimpleFeatureType = null
  private var featureStore: FeatureStore[SimpleFeatureType, SimpleFeature] = null

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {}

  override def execute(input: Tuple): Unit = {
    val sf = buildFeature(input)
    val fc = new DefaultFeatureCollection(sft.getTypeName, sft)
    fc.add(sf)
    featureStore.addFeatures(fc)
    println(s"Inserted feature for $getSinkName" + DataUtilities.encodeFeature(sf))
  }


  def storeFeature(sf: SimpleFeature) = {
    val fc = new DefaultFeatureCollection(sft.getTypeName, sft)
    fc.add(sf)
    featureStore.addFeatures(fc)
  }

  override def prepare(stormConf: java.util.Map[_, _],
                       context: TopologyContext,
                       collector: OutputCollector): Unit = {
    def getStr(param: String) = stormConf.get(param).asInstanceOf[String]

    this.outputCollector = collector
    this.sft = parseSft(stormConf)

    import org.locationtech.geomesa.analytic.storm.GeoMesaSinkParams._
    import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
    val dsParams = Map(
      tableNameParam.getName  -> getStr(Catalog),
      userParam.getName       -> getStr(User),
      passwordParam.getName   -> getStr(Password),
      zookeepersParam.getName -> getStr(Zookeepers),
      instanceIdParam.getName -> getStr(Instance),
      mockParam.getName       -> getStr(Mock)
    ).asJava
    val ds = DataStoreFinder.getDataStore(dsParams)
    ds.createSchema(sft)
    this.featureStore = ds.getFeatureSource(sft.getTypeName).asInstanceOf[FeatureStore[SimpleFeatureType, SimpleFeature]]

    println(s"Set up GeoMesa Sink $getSinkName")
  }

  def parseSft(stormConf: java.util.Map[_, _]): SimpleFeatureType

  def getSinkName: String

  def buildFeature(input: Tuple): SimpleFeature

  def getSft = sft

}

object GeoMesaSinkParams {
  val PRE        = "geomesa.sink"
  val Catalog    = s"$PRE.catalog"
  val User       = s"$PRE.user"
  val Password   = s"$PRE.password"
  val Instance   = s"$PRE.instance"
  val Zookeepers = s"$PRE.zookeepers"
  val Mock       = s"$PRE.mock"
}


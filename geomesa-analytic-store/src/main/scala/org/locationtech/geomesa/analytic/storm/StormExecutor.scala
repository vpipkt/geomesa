package org.locationtech.geomesa.analytic.storm

import java.util.UUID

import backtype.storm.topology.TopologyBuilder
import backtype.storm.{Config, LocalCluster}
import org.locationtech.geomesa.analytic.storm.GeoMesaSinkParams._

object StormExecutor {

  val AlertSftName = "alertRattai"
  val AlertSftDef = "alertId:String:index=true,site:String:index=true,dtg:Date,*geom:Geometry:srid=4326"

  val TimeSeriesSftName = "timeseriesRattai"
  val TimeSeriesSftDef = "site:String:index=true,count:Int:index=true,isAlert:Boolean:index=true,dtg:Date,*geom:Geometry:srid=4326"

  def runLocalCluster(conf: ActivityConfig) = {

    val cluster = new LocalCluster()

    startSiteMonitoring(conf.inputTopic, conf.alertTopic, conf.timeseriesTopic, cluster)

    while(true) {
      Thread.sleep(1000)
    }

  }

  override def hashCode(): Int = super.hashCode()

  def startSiteMonitoring(inTopic: String, alertTopic: String, tsTopic: String, cluster: LocalCluster): String = {
    val builder = new TopologyBuilder
    builder.setSpout("kafkaSpout", new KafkaSpout)
    builder.setBolt("anomalyDetector", new AnomalyDetectorBolt, 1).shuffleGrouping("kafkaSpout")
    builder.setBolt("kafkaAlertSink", new KafkaSinkBolt, 1).shuffleGrouping("anomalyDetector", "alerts")
    builder.setBolt("geomesaSink", new GeoMesaAlertSink, 1).shuffleGrouping("anomalyDetector", "alerts")
    builder.setBolt("timeseriesSink", new GeoMesaTimeSeriesSink, 1).shuffleGrouping("anomalyDetector", "timeseries")

    val conf = new Config()
    conf.setDebug(false)
    conf.setNumWorkers(3)
    conf.put("broker", "localhost:9092")
    conf.put("topic.in", inTopic)
    conf.put("topic.alert", alertTopic)
    conf.put("timeseries.window", "4")

    Seq(
      GeoMesaAlertSinkParams.SftName   -> AlertSftName,
      GeoMesaAlertSinkParams.Sft       -> AlertSftDef,
      GeoMesaTimeSeriesSink.SftName    -> TimeSeriesSftName,
      GeoMesaTimeSeriesSink.Sft        -> TimeSeriesSftDef,
      Catalog    -> "ahulbert_alert",
      User       -> "root",
      Password   -> "secret",
      Instance   -> "dcloud",
      Zookeepers -> "dzoo1,dzoo2,dzoo3"
    ).foreach { case (k, v) => conf.put(k, v) }

    val topologyName = UUID.randomUUID().toString
    cluster.submitTopology(topologyName, conf, builder.createTopology())

    topologyName
  }

  def main(args: Array[String]) = {
    val parser = new scopt.OptionParser[ActivityConfig]("java -jar activity-indicator.jar") {
      head("ActivityIndicator", "1.0")
      opt[String]('i', "input-topic") required() action { (x, c) =>
        c.copy(inputTopic = x) } text("Input Kafka Topic")
      opt[String]('a', "alert-topic") required() action { (x, c) =>
        c.copy(alertTopic = x) } text("Alert Topic")
      opt[String]('t', "ts-topic") action { (x, c) =>
        c.copy(timeseriesTopic = x) } text("TimeSeries Topic")
      opt[Int]('w', "window") action { (x, c) =>
        c.copy(window = x) } text("TimeSeries Window (days)")
    }
    // parser.parse returns Option[C]
    parser.parse(args, ActivityConfig()) map { config =>
      runLocalCluster(config)
    } getOrElse {
      // arguments are bad, error message will have been displayed
    }
  }

  case class ActivityConfig( inputTopic: String = null,
                             alertTopic: String = null,
                             timeseriesTopic: String = null,
                             window: Int = 20)
}

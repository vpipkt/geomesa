package org.locationtech.geomesa.analytic.storm

import java.util.UUID

import backtype.storm.topology.TopologyBuilder
import backtype.storm.{StormSubmitter, Config, LocalCluster}
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.analytic.storm.GeoMesaSinkParams._

object StormExecutor {

  val AlertSftDef = "alertId:String:index=true,site:String:index=true,dtg:Date,*geom:Geometry:srid=4326"
  val TimeSeriesSftDef = "site:String:index=true,count:Int:index=true,isAlert:Boolean:index=true,alertId:String:index=true,dtg:Date,*geom:Geometry:srid=4326"

  def runLocalCluster(aconf: ActivityConfig) = {

    val stormConf = getConf(aconf)
    val name = aconf.name
    val topology = getTopology(aconf)

    aconf.mode match {
      case "local" =>
        val cluster = new LocalCluster()
        cluster.submitTopology(name, stormConf, topology)
        while(true) {
          Thread.sleep(1000)
        }
      case "cluster" =>
        StormSubmitter.submitTopology(name, stormConf, topology)
    }

  }

  override def hashCode(): Int = super.hashCode()

  def getTopology(aconf: ActivityConfig) = {
    val builder = new TopologyBuilder
    builder.setSpout("kafkaSpout", new KafkaSpout)
    builder.setBolt("anomalyDetector", new AnomalyDetectorBolt, 1).shuffleGrouping("kafkaSpout")
    builder.setBolt("kafkaAlertSink", new KafkaSinkBolt, 1).shuffleGrouping("anomalyDetector", "alerts")
    builder.setBolt("geomesaSink", new GeoMesaAlertSink, 1).shuffleGrouping("anomalyDetector", "alerts")
    builder.setBolt("timeseriesSink", new GeoMesaTimeSeriesSink, 1).shuffleGrouping("anomalyDetector", "timeseries")

    builder.createTopology()
  }

  def getConf(aconf: ActivityConfig) = {
    val conf = new Config()
    conf.setDebug(false)
    conf.setNumWorkers(10)
    conf.put("broker", aconf.broker)
    conf.put("topic.in", aconf.inputTopic)
    conf.put("topic.alert", aconf.alertTopic)
    conf.put("timeseries.window", aconf.window.toString)
    conf.put("input.quoted", aconf.quotedInput.toString)
    conf.put("input.date.format", aconf.dateFormat)

    Seq(
      GeoMesaAlertSinkParams.SftName -> aconf.alertSftName,
      GeoMesaAlertSinkParams.Sft -> AlertSftDef,
      GeoMesaTimeSeriesSink.SftName ->  aconf.timeSeriesSftName,
      GeoMesaTimeSeriesSink.Sft -> TimeSeriesSftDef,
      Catalog -> aconf.catalog,
      User -> aconf.user,
      Password -> aconf.password,
      Instance -> aconf.instance,
      Zookeepers -> aconf.zookeepers,
      Mock       -> aconf.mockGeomesa.toString
    ).foreach { case (k, v) => conf.put(k, v)}

    conf
  }

  def main(args: Array[String]) = {
    val parser = new scopt.OptionParser[ActivityConfig]("java -jar activity-indicator.jar") {
      head("ActivityIndicator", "1.0")
      opt[String]('i', "input-topic") required() action { (x, c) =>
        c.copy(inputTopic = x) } text("Input Kafka Topic")
      opt[Boolean]('q', "quoted-input") action { (x, c) =>
        c.copy(quotedInput = x) } text("True/False whether input is quoted (default false)")
      opt[String]('a', "alert-topic") required() action { (x, c) =>
        c.copy(alertTopic = x) } text("Alert Topic")
      opt[String]('t', "ts-topic") action { (x, c) =>
        c.copy(timeseriesTopic = x) } text("TimeSeries Topic")
      opt[Int]('w', "window") action { (x, c) =>
        c.copy(window = x) } text("TimeSeries Window (days)")
      opt[String]('d', "dateFmt") action { (x, c) =>
        c.copy(dateFormat = x) } text("Input date format (for joda DateTime) (default yyyy-MM-dd'T'HH:mm:ss.SSSZ)")
      opt[String]('m', "mode") required() action { (x, c) =>
        c.copy(mode = x) } text("mode (local or cluster)")
      opt[String]('n', "name") required() action { (x, c) =>
        c.copy(name = x) } text("topology name")
      opt[String]("alertSftName") required() action { (x, c) =>
        c.copy(alertSftName = x) } text("alert simple feature type name / layer name")
      opt[String]("timeSeriesSftName") required() action { (x, c) =>
        c.copy(timeSeriesSftName = x) } text("timeSeriesSftName simple feature type name / layer name")
      opt[String]('z', "zookeepers") required() action { (x, c) =>
        c.copy(zookeepers = x) } text("zookeepers")
      opt[String]('u', "user") required() action { (x, c) =>
        c.copy(user = x) } text("accumulo user")
      opt[String]('p', "password") required() action { (x, c) =>
        c.copy(password = x) } text("accumulo password")
      opt[String]("instance") required() action { (x, c) =>
        c.copy(instance = x) } text("accumulo instance")
      opt[String]("catalog") required() action { (x, c) =>
        c.copy(catalog = x) } text("geomesa catalog table")
      opt[String]("broker") required() action { (x, c) =>
        c.copy(broker = x) } text("kafka brokers")
      opt[Boolean]("mock-geomesa") action { (x, c) =>
        c.copy(mockGeomesa = x) } text("true/false use mock geomesa (default false)")
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
                             window: Int = 20,
                             quotedInput: Boolean = false,
                             dateFormat: String = "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
                             mode: String = null,
                             name: String = null,
                             alertSftName: String = null,
                             timeSeriesSftName: String = null,
                             zookeepers: String = null,
                             user: String = null,
                             password: String = null,
                             instance: String = null,
                             catalog: String = null,
                             broker: String = null,
                             mockGeomesa: Boolean = false)
}

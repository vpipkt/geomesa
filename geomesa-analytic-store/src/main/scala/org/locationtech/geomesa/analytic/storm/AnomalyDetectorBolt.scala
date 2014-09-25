package org.locationtech.geomesa.analytic.storm

import java.util.{Date, UUID, Map => JMap}

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple, Values}
import org.joda.time.DateTime
import org.locationtech.geomesa.analytic.{DayInterval, TimeSeries}


class AnomalyDetectorBolt() extends BaseRichBolt {
  private var outputCollector: OutputCollector = null
  private var tsMap: collection.mutable.HashMap[String, TimeSeries] = null
  private var window: String = null


  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declareStream("alerts", new Fields("alertID", "site", "time"))
    declarer.declareStream("timeseries", new Fields("date", "site", "count", "alert"))
  }

  override def execute(input: Tuple) = {
    val time = new DateTime(input.getValue(0).asInstanceOf[Date])
    val site = input.getString(1)
    val count = input.getInteger(2)

    if (!tsMap.contains(site)) tsMap(site) = new TimeSeries(DayInterval, window.toInt)

    val addResult = tsMap(site).addObs(time, count)

    // Process alert
    if (addResult._1) {
      println("Creating alert at time "+time)
      val alertId = UUID.randomUUID
      outputCollector.emit("alerts", new Values(
        alertId.asInstanceOf[AnyRef],
        site.asInstanceOf[AnyRef],
        time.toDate.asInstanceOf[AnyRef]
      ))
    }

    // Process TimeSeries
    addResult._2 match {
      case Some(ts) =>
        outputCollector.emit("timeseries", new Values(
          ts.dt.toDate.asInstanceOf[AnyRef],
          site.asInstanceOf[AnyRef],
          ts.count.asInstanceOf[AnyRef],
          ts.alert.asInstanceOf[AnyRef]
        ))
      case _ =>
    }
  }

  override def prepare(conf: JMap[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    outputCollector = collector
    window = conf.get("timeseries.window").asInstanceOf[String]
    tsMap = collection.mutable.HashMap.empty[String, TimeSeries]
  }
}

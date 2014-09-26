package org.locationtech.geomesa.analytic.storm

import java.util.{Date, Map => JMap}

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple, Values}
import org.joda.time.DateTime
import org.locationtech.geomesa.analytic.{DayInterval, TimeSeries, TimeSeriesData}


class AnomalyDetectorBolt() extends BaseRichBolt {
  var outputCollector: OutputCollector = null
  var tsMap: Map[String, TimeSeries] = null
  var window: String = null


  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declareStream("alerts", new Fields("alertID", "site", "time"))
    declarer.declareStream("timeseries", new Fields("date", "site", "count", "alert", "alertId"))
  }

  override def execute(input: Tuple) = {
    val time = new DateTime(input.getValue(0).asInstanceOf[Date])
    val site = input.getString(1)
    val count = input.getInteger(2)

    val sendTs: (TimeSeriesData) => Unit = (ts: TimeSeriesData) => {
      outputCollector.emit("timeseries", new Values(
        ts.dt.toDate.asInstanceOf[AnyRef],
        site.asInstanceOf[AnyRef],
        ts.count.asInstanceOf[AnyRef],
        ts.alert.asInstanceOf[AnyRef],
        ts.alertId.getOrElse(null).asInstanceOf[AnyRef]
      ))
    }

    if (!tsMap.contains(site)) tsMap = tsMap + (site -> new TimeSeries(DayInterval, window.toInt, saveFunc=sendTs))

    val addResult = tsMap(site).addObs(time, count)

    // Process alert
    if (addResult._1) {
      println("Creating alert at time "+time)
      outputCollector.emit("alerts", new Values(
        addResult._2.get.asInstanceOf[AnyRef],
        site.asInstanceOf[AnyRef],
        time.toDate.asInstanceOf[AnyRef]
      ))
    }
  }


  override def prepare(conf: JMap[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    outputCollector = collector
    window = conf.get("timeseries.window").asInstanceOf[String]
    tsMap = Map[String, TimeSeries]()
  }
}

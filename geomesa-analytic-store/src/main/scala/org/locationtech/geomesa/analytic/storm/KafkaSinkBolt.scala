package org.locationtech.geomesa.analytic.storm

import java.util
import java.util.{Date, UUID, Properties}

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class KafkaSinkBolt extends BaseRichBolt {

  private var producer: Producer[String, Array[Byte]] = null
  private var topic: String = null

  override def declareOutputFields(p1: OutputFieldsDeclarer): Unit = {}

  override def execute(p1: Tuple): Unit = {
    val alertId = p1.getString(0)
    val site = p1.getString(1)
    val time = new DateTime(p1.getValue(2).asInstanceOf[Date]).toString(ISODateTimeFormat.dateTime())

    val str = List(alertId, site, time).mkString(",")

    producer.send(new KeyedMessage[String, Array[Byte]](topic, str.getBytes))
  }

  override def prepare(stormConf: util.Map[_, _],
                       context: TopologyContext,
                       outputCollector: OutputCollector): Unit = {
    val broker: String = stormConf.get("broker").asInstanceOf[String]
    this.topic = stormConf.get("topic.alert").asInstanceOf[String]
    this.producer = new Producer[String, Array[Byte]](ProducerPropertiesBuilder.build(broker))
  }
}

object ProducerPropertiesBuilder {
  def build(broker: String) = {
    val props = new Properties()
    props.put("metadata.broker.list", broker)
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("batch.size", "1")
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    props.put("producer.type", "sync")
    new ProducerConfig(props)
  }
}

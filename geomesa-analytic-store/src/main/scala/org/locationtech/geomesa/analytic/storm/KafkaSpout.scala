package org.locationtech.geomesa.analytic.storm

import java.util
import java.util.Properties

import backtype.storm.Config
import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{IRichSpout, OutputFieldsDeclarer}
import backtype.storm.tuple.{Fields, Values}
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

class KafkaSpout() extends IRichSpout {
  private var collector: SpoutOutputCollector = null
  private var consumerConnector: ConsumerConnector = null
  private var dtParser: DateTimeFormatter = null
  private var topic: String = null

  private var stream: KafkaStream[_, Array[Byte]] = null

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("date", "site", "count"))
  }

  override def getComponentConfiguration: util.Map[String, AnyRef] = {
    val conf = new Config()
    conf.setMaxTaskParallelism(1)
    conf
  }

  override def nextTuple(): Unit =
    stream foreach {
      case msg: MessageAndMetadata[_, _] =>

        try {
          val str = new String(msg.message)
          //println(s"Parsing $str")
          val arr = str.split(",")

          val date = dtParser.parseDateTime(arr(0))
          val site = arr(1)
          val count = arr(2).toInt

          collector.emit(new Values(date.toDate.asInstanceOf[AnyRef], site.asInstanceOf[AnyRef], count.asInstanceOf[AnyRef]))
        } catch {
          case e: Exception =>
            println("error: " + e.getMessage)
        }

    }

  override def activate(): Unit = {}

  override def deactivate(): Unit = {}

  override def close(): Unit = {
    println("closing spout")
    consumerConnector.shutdown()
  }

  override def fail(msgId: scala.Any): Unit = {}

  override def ack(msgId: scala.Any): Unit = {}

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    this.collector = collector
    consumerConnector = Consumer.create(ConsumerPropertiesBuilder.build())
    this.dtParser = ISODateTimeFormat.dateTime()
    this.topic = conf.get("topic.in").asInstanceOf[String]

    val consumerMap = consumerConnector.createMessageStreams(Map(topic -> 1), new DefaultDecoder(), new DefaultDecoder())
    consumerMap.get(topic).map { lst =>
      lst.headOption.map { ks =>
        this.stream = ks
        println("set stream for topic " + topic)
      }
    }
  }

}

object ConsumerPropertiesBuilder {
  def build() = {
    val props = new Properties()
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "ahulbert_foobar1")
    new ConsumerConfig(props)
  }
}

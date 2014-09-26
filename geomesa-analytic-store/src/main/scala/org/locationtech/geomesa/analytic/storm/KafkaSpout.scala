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
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

class KafkaSpout() extends IRichSpout {
  private var collector: SpoutOutputCollector = null
  private var consumerConnector: ConsumerConnector = null
  private var dtParser: DateTimeFormatter = null
  private var topic: String = null
  private var inputQuoted: Boolean = false

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
          val arr = str.split(",").map { s =>
            if (inputQuoted) s.drop(1).dropRight(1)
            else s
          }

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
    consumerConnector = Consumer.create(ConsumerPropertiesBuilder.build(conf.get("kafka.zoo").asInstanceOf[String]))
    val dateFormat = conf.get("input.date.format").asInstanceOf[String]
    this.dtParser = DateTimeFormat.forPattern(dateFormat)
    this.topic = conf.get("topic.in").asInstanceOf[String]
    this.inputQuoted = conf.get("input.quoted").asInstanceOf[String].toBoolean


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
  def build(zk: String) = {
    val props = new Properties()
    props.put("zookeeper.connect", zk)
    props.put("group.id", "ahulbert_foobar1")
    new ConsumerConfig(props)
  }
}

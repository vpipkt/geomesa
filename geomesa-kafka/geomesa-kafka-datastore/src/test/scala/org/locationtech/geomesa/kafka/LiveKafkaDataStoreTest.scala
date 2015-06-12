/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kafka

import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.{Collections, Date}
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.Logging
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.ZkClient
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import org.locationtech.geomesa.utils.geotools.Conversions._

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class LiveKafkaDataStoreTest extends Specification with Logging {

  skipped("integration test")

  val brokers = "kafka1:9092,kafka1:9093,kafka1:9094"
  val zoos = "zoo1,zoo2,zoo3"
  val zkPath = "/geomesa/kafka/my-test"

  val params = Map(
    "brokers"    -> brokers,
    "zookeepers" -> zoos,
    "zkPath"     -> zkPath
  )
  val consumerParams = params ++ Map("isProducer" -> false)
  val producerParams = params ++ Map("isProducer" -> true)

  val ff = CommonFactoryFinder.getFilterFactory2
  val gf = JTSFactoryFinder.getGeometryFactory

  val sftName = "my-test-rate3"

  val sftConf = s"""
      |{
      |  type-name = "$sftName"
      |  fields = [
      |    { name = "dtg1",  type = "Date",   index = true },
      |    { name = "str1",  type = "String", index = true },
      |    { name = "str2",  type = "String", index = true },
      |    { name = "lat",   type = "Double", index = true },
      |    { name = "lon",   type = "Double", index = true },
      |    { name = "dbl1",  type = "Double", index = true },
      |    { name = "dbl2",  type = "Double", index = true },
      |    { name = "dbl3",  type = "Double", index = true },
      |    { name = "str3",  type = "String", index = true },
      |    { name = "str4",  type = "String", index = true },
      |    { name = "str5",  type = "String", index = true },
      |    { name = "str6",  type = "String", index = true },
      |    { name = "time",  type = "Date",   index = true },
      |    { name = "str7",  type = "String", index = true },
      |    { name = "str8",  type = "String", index = true },
      |    { name = "str9",  type = "String", index = true },
      |    { name = "str10", type = "String", index = true },
      |    { name = "geom",  type = "LineString",  index = true, srid = 4326, default = true },
      |    { name = "str11", type = "String", index = true },
      |    { name = "str12", type = "String", index = true },
      |    { name = "dtg",   type = "Date",   index = true }
      |  ]
      |}
    """.stripMargin

  val schema = KafkaDataStoreHelper.createStreamingSFT(SimpleFeatureTypes.createType(ConfigFactory.parseString(sftConf)), zkPath)

  "KafkaDataSource" should {
    "produce and consume" in {
      val consumer = DataStoreFinder.getDataStore(consumerParams).asInstanceOf[KafkaDataStore]
      val producer = DataStoreFinder.getDataStore(producerParams).asInstanceOf[KafkaDataStore]

//      producer.createSchema(schema)

      var loop = true
      val written = new AtomicInteger(0)

      // create the consumerFC first so that it is ready to receive features from the producer
      val consumerFs = consumer.getFeatureSource(sftName)
      val producerFs = producer.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]

      val values = Array(
        new Date(),
        "testtest",
        "testtest",
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        "testtest",
        "testtest",
        "testtest",
        "testtest",
        new Date(),
        "testtest",
        "testtest",
        "testtest",
        "testtest",
        null,
        "testtest",
        "testtest",
        new Date()
      ).asInstanceOf[Array[AnyRef]]

      val r = new Random()
      def lat = r.nextDouble() * 180 - 90
      def lon = r.nextDouble() * 360 - 180
      val start = System.currentTimeMillis()

      val ids = new LinkedBlockingQueue[String]()
      val fw = producer.getFeatureWriter(sftName, null, Transaction.AUTO_COMMIT)
      val writer = new Thread(new Runnable() {
        override def run() = {
          while (loop) {
            val sf = fw.next()
            ids.put(sf.getID)
            values(17) = WKTUtils.read(s"LINESTRING($lat $lon, $lat $lon, $lat $lon, $lat $lon, $lat $lon, $lat $lon, $lat $lon, $lat $lon, $lat $lon, $lat $lon)")
            sf.setAttributes(values)
            fw.write()
            written.incrementAndGet()
          }
        }
      })

//      val remover = new Thread(new Runnable() {
//        override def run() = {
//          while (loop && written.get() < 2000000) {
//            Thread.sleep(1000)
//          }
//          while (loop) {
//            val id = ids.take()
//            producerFs.removeFeatures(ECQL.toFilter(s"IN('$id')"))
//          }
//        }
//      })

      val reader = new Thread(new Runnable() {
        override def run() = {
          while (loop) {
            val features = consumerFs.getFeatures(ECQL.toFilter("bbox(Location,-180,-90,180,90)")).features()
//            val count = features.length
            if (features.hasNext) {
              features.next()
            }
            features.close()
            val count = consumerFs.getCount(new Query(sftName))
            val behind = ids.size() - count
            if (behind > 2000) {
              println(s"${(System.currentTimeMillis() - start) / 1000}: got $count behind by $behind")
            }
            Thread.sleep(100)
          }
        }
      })

      writer.start()
      reader.start()
//      remover.start()

      Thread.sleep(1200000)
      loop = false

      writer.join()
      reader.join()
//      remover.join()

      success
    }
  }
}
//123: got 891046 behind by 8067
//340: got 2323723 behind by 239985
// new with querying 120: got 851440 behind by 91480
// new with querying 120: got 897791 behind by 4367
// new sans querying 120: got 882828 behind by 1536
// new sans querying 120: got 795083 behind by 162684
// fix sans querying 120: got 893795 behind by 1643
// fix sans querying 120: got 889462 behind by 1589

//Total time: 6345 ms. Percent of time - cache-insert: 13.0% 1468059 times at 0.0006 ms avg, qt-insert: 73.5% 1468068 times at 0.0032 ms avg, qt-remove: 13.5% 1468068 times at 0.0006 ms avg
//192: got 1471096 behind by 750
//Total time: 51919 ms. Percent of time - cache-insert: 1.7% 1542212 times at 0.0006 ms avg, qt-insert: 9.3% 1542212 times at 0.0031 ms avg, qt-remove: 89.0% 1542212 times at 0.0300 ms avg
//249: got 1541759 behind by 351223
package org.locationtech.geomesa.accumulo.util

import java.util.concurrent.TimeUnit

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.PropAndDefault
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class GeoMesaBatchWriterConfigTest extends Specification {
  val bwc = GeoMesaBatchWriterConfig.buildBWC    // Builds new BWC which has not been mutated by some other test.

  import GeomesaSystemProperties.BatchWriterProperties

  sequential

  "GeoMesaBatchWriterConfig" should {
    "have defaults set" in {
      bwc.getMaxMemory                         must be equalTo BatchWriterProperties.WRITER_MEMORY.default.toLong
      bwc.getMaxLatency(TimeUnit.MILLISECONDS) must be equalTo BatchWriterProperties.WRITER_LATENCY_MILLIS.default.toLong
      bwc.getMaxWriteThreads                   must be equalTo BatchWriterProperties.WRITER_THREADS.default.toInt
    }
  }

  "GeoMesaBatchWriterConfig" should {
    "respect system properties" in {
      val latencyProp = "1234"
      val memoryProp  = "2345"
      val threadsProp = "25"
      val timeoutProp = "33"

      BatchWriterProperties.WRITER_LATENCY_MILLIS.set(latencyProp)
      BatchWriterProperties.WRITER_MEMORY.set(memoryProp)
      BatchWriterProperties.WRITER_THREADS.set(threadsProp)
      BatchWriterProperties.WRITE_TIMEOUT.set(timeoutProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC

      BatchWriterProperties.WRITER_LATENCY_MILLIS.clear()
      BatchWriterProperties.WRITER_MEMORY.clear()
      BatchWriterProperties.WRITER_THREADS.clear()
      BatchWriterProperties.WRITE_TIMEOUT.clear()

      nbwc.getMaxLatency(TimeUnit.MILLISECONDS) must be equalTo java.lang.Long.parseLong(latencyProp)
      nbwc.getMaxMemory                         must be equalTo java.lang.Long.parseLong(memoryProp)
      nbwc.getMaxWriteThreads                   must be equalTo java.lang.Integer.parseInt(threadsProp)
      nbwc.getTimeout(TimeUnit.SECONDS)         must be equalTo java.lang.Long.parseLong(timeoutProp)
    }
  }

  "fetchProperty" should {
    "retrieve a long correctly" in {
      System.setProperty("foo", "123456789")
      val ret = GeoMesaBatchWriterConfig.fetchProperty(PropAndDefault("foo", null))
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l))
    }

    "return None correctly" in {
      GeoMesaBatchWriterConfig.fetchProperty(PropAndDefault("bar", null)) should equalTo(None)
    }

    "return None correctly when the System property is not parseable as a Long" in {
      System.setProperty("baz", "fizzbuzz")
      val ret = GeoMesaBatchWriterConfig.fetchProperty(PropAndDefault("foo", null))
      System.clearProperty("baz")
      ret should equalTo(None)
    }
  }
}

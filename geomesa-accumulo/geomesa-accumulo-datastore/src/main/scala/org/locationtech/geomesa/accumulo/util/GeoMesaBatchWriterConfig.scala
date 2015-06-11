package org.locationtech.geomesa.accumulo.util

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchWriterConfig
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.PropAndDefault

import scala.util.Try

object GeoMesaBatchWriterConfig extends Logging {

  protected[util] def fetchProperty(prop: PropAndDefault): Option[Long] =
    for { p <- Option(prop.get); num <- Try(java.lang.Long.parseLong(p)).toOption } yield num

  protected[util] def buildBWC: BatchWriterConfig = {
    import GeomesaSystemProperties.BatchWriterProperties

    val bwc = new BatchWriterConfig

    val latency = fetchProperty(BatchWriterProperties.WRITER_LATENCY_MILLIS)
        .getOrElse(GeomesaSystemProperties.BatchWriterProperties.WRITER_LATENCY_MILLIS.default.toLong)
    logger.trace(s"GeoMesaBatchWriter config: maxLatency set to $latency milliseconds.")
    bwc.setMaxLatency(latency, TimeUnit.MILLISECONDS)

    // TODO: Allow users to specify member with syntax like 100M or 50k.
    // https://geomesa.atlassian.net/browse/GEOMESA-735
    val memory = fetchProperty(BatchWriterProperties.WRITER_MEMORY)
        .getOrElse(BatchWriterProperties.WRITER_MEMORY.default.toLong)
    logger.trace(s"GeoMesaBatchWriter config: maxMemory set to $memory bytes.")
    bwc.setMaxMemory(memory)

    val threads = fetchProperty(BatchWriterProperties.WRITER_THREADS).map(_.toInt)
        .getOrElse(BatchWriterProperties.WRITER_THREADS.default.toInt)
    logger.trace(s"GeoMesaBatchWriter config: maxWriteThreads set to $threads.")
    bwc.setMaxWriteThreads(threads.toInt)

    fetchProperty(BatchWriterProperties.WRITE_TIMEOUT).foreach { timeout =>
      logger.trace(s"GeoMesaBatchWriter config: maxTimeout set to $timeout seconds.")
      bwc.setTimeout(timeout, TimeUnit.SECONDS)
    }

    bwc
  }

  def apply(): BatchWriterConfig = buildBWC
}

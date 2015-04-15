package org.locationtech.geomesa.stream.datastore

import java.nio.charset.StandardCharsets

import com.google.common.io.Resources
import org.apache.commons.io.IOUtils
import org.apache.commons.net.DefaultSocketFactory
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class StreamDataStoreTest extends Specification {

  "StreamDataStore" should {

    val sourceConf =
      """
        |{
        |  type         = "generic"
        |  source-route = "netty4:tcp://localhost:5899?textline=true"
        |  sft          = {
        |                   type-name = "testdata"
        |                   fields = [
        |                     { name = "label",     type = "String" }
        |                     { name = "geom",      type = "Point",  index = true, srid = 4326, default = true }
        |                     { name = "dtg",       type = "Date",   index = true }
        |                   ]
        |                 }
        |  threads      = 4
        |  converter    = {
        |                   id-field = "md5(string2bytes($0))"
        |                   type = "delimited-text"
        |                   format = "DEFAULT"
        |                   fields = [
        |                     { name = "label",     transform = "trim($1)" }
        |                     { name = "geom",      transform = "point($2::double, $3::double)" }
        |                     { name = "dtg",       transform = "datetime($4)" }
        |                   ]
        |                 }
        |}
      """.stripMargin

    val ds = DataStoreFinder.getDataStore(
      Map(
        StreamDataStoreParams.STREAM_DATASTORE_CONFIG.key -> sourceConf,
        StreamDataStoreParams.CACHE_TIMEOUT.key -> Integer.valueOf(2)
      ))

    "be built from a conf string" >> {
      ds must not be null
    }

    val fs = ds.getFeatureSource("testdata")

    "handle new data" >> {
      val url = Resources.getResource("testdata.tsv")
      val lines = Resources.readLines(url, StandardCharsets.UTF_8)
      val socketFactory = new DefaultSocketFactory
      Future {
        val socket = socketFactory.createSocket("localhost", 5899)
        val os = socket.getOutputStream
        IOUtils.writeLines(lines, IOUtils.LINE_SEPARATOR_UNIX, os)
        os.flush()
        // wait for data to arrive at the server
        Thread.sleep(4000)
        os.close()
      }

      Thread.sleep(1000)
      fs.getFeatures(Filter.INCLUDE).features().hasNext must beTrue
    }

    "expire data after the appropriate amount of time" >> {
      Thread.sleep(3000)
      fs.getFeatures(Filter.INCLUDE).features().hasNext must beFalse
    }
  }
}
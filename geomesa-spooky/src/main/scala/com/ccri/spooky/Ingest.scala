package com.ccri.spooky

import java.util.UUID

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.format.DateTimeFormat
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.util.SftBuilder

import scala.collection.JavaConversions._
import scala.io.Source

object Ingest {

  def main(args: Array[String]): Unit = {

    println("file: "+args(0))
    val sft =
      new SftBuilder()
        .listType[Int]("featureVec")
        .date("dtg", default = true)
        .point("geom", default=true)
        .build("rawImageVec")

    val geoFac = new GeometryFactory()
    val dt = DateTimeFormat.forPattern("yyyy:MM:dd HH:mm:ss").withZoneUTC()

    val ds = DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "FOO",
      "zookeepers"        -> "BLAH",
      "user"              -> "NOPE",
      "password"          -> "YOUWISH",
      "tableName"         -> "spooky_catalog")).asInstanceOf[AccumuloDataStore]

    ds.createSchema(sft)
    Thread.sleep(2000)
    val writer = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)

    //14.5990429137,120.752524883,2014:08:05 15:23:37,211|167|222|83|176|60|112|135|127|63|170|116|34|34|207|123|165|207|108|16|190|249|171|99|186|186|84|67|200|81|235|13|
    Source.fromFile(args(0)).getLines().foreach{ line =>
      println("parsing "+ line)
      val arr = line.split(",")
      val lat = arr(0).toDouble
      val lon = arr(1).toDouble
      val date = dt.parseDateTime(arr(2)).toDate
      println(arr(3).dropRight(1).split("|"))
      val features = arr(3).dropRight(1).split("[\\|]").map(_.toInt).toList

      val geom = geoFac.createPoint(new Coordinate(lon, lat))

      val next = writer.next()
      next.getIdentifier.asInstanceOf[FeatureIdImpl].setID(UUID.randomUUID().toString)
      next.setAttributes(Array(features, date, geom))
      writer.write()
    }
    writer.close()
  }
}

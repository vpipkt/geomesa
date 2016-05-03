package org.locationtech.geomesa.compute.spark.analytics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
//import org.geotools.api
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.filter.SortByImpl
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point, LineString, Geometry}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import org.locationtech.geomesa.utils.text.WKTUtils
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConversions._

object PointToLinestring {

  val inParams = Map(
    "instanceId" -> "local",
    "zookeepers" -> "localhost:2181",
    "user" -> "root",
    "password" -> "secret",
    "tableName" -> "geomesa121.osm")

  val outParams = Map(
    "instanceId" -> "local",
    "zookeepers" -> "localhost:2181",
    "user" -> "root",
    "password" -> "secret",
    "tableName" -> "geomesa121.osmtracks")

  val pointTypeName = "osm-csv"
  val pointGeom = "geom"
  val date = "dtg"
  val trackId = "trackId"

  val trackSft = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.setName(pointTypeName + "-tracks")
    builder.add(trackId, classOf[String])
   // builder.add("dtg", classOf[Date])
    //builder.add("endDtg", classOf[Date])
    builder.add("geom", classOf[LineString], 4326)
    builder.buildFeatureType()
  }

  def attrToString(attr: Any): String = attr match {
    case null => ""
    case s: String => s
    case d: Date   =>  ISODateTimeFormat.basicDateTime().withZoneUTC().print(d.getTime)
    case g: Geometry => WKTUtils.write(g)
    case a => a.toString
  }

  def main(args: Array[String]) {
    // Get a handle to the data store
    val ds = DataStoreFinder.getDataStore(inParams).asInstanceOf[AccumuloDataStore]

    // Construct a query of all features (INCLUDE)
    val q = new Query(pointTypeName)

    /*q.setSortBy(Array(
      new SortByImpl(date, org.opengis.filter.sort.SortOrder.ASCENDING))
    )*/

    // Configure Spark
    val sc = new SparkContext(GeoMesaSpark.init(new SparkConf(true), ds))

    // Create an RDD of point features
    val pointRDD = GeoMesaSpark.rdd(new Configuration, sc, inParams, q, None)

    // Convert RDD[SimpleFeature] to RDD[(String, SimpleFeature)] where
    //   the first element of the tuple is the track identifier
    val tidAndFeature = pointRDD.mapPartitions { iter =>
      val ff = CommonFactoryFinder.getFilterFactory2
      val exp = ff.property(trackId)
      iter.map { f => (exp.evaluate(f).asInstanceOf[String], f) }
    }

    // group by feature id:
    val g = tidAndFeature.groupByKey()
/*    val sorted = g.mapPartitions { kv =>
      kv.map{ i => (i._1, i._2.map{
        f => (f.getAttribute(date), f)
      } )
      }
    }*/

    val gf = new GeometryFactory

    val idLinestrings = g.mapPartitions { iter =>
      iter.map { pair =>
        var ls = gf.createLineString(Array[Coordinate]())
        val coords = pair._2.map(f => f.getDefaultGeometry.asInstanceOf[Point].getCoordinate).toArray
        if (coords.length >1) {
          ls = gf.createLineString(coords)
        }
          (pair._1, ls)
      }
    }

    val sfts = idLinestrings.mapPartitions{ iter =>
      iter.map { pair => {
        new ScalaSimpleFeature(pair._1.toString, trackSft, Array[AnyRef](pair._1, pair._2)).asInstanceOf[SimpleFeature]
      }
      }
    }

    // seems like a good idea, but has some problem  sfts.cache()

    println("\n\nINFO: Created  "  + sfts.count + " linestrings from " + pointRDD.count + " points.\n\n")

    sfts.mapPartitions{ ss =>
      ss.map(sf => sf.getID + sf.getAttributes().map(attrToString))
    }.saveAsTextFile("hdfs://localhost:9000/user/root/" + trackSft.getTypeName )

    val     outds = DataStoreFinder.getDataStore(outParams).asInstanceOf[AccumuloDataStore]
    outds.createSchema(trackSft)
    GeoMesaSpark.save(sfts, outParams, trackSft.getTypeName)
  }
}


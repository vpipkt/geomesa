package org.locationtech.geomesa.compute.spark.analytics

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

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

  // Get a handle to the data stores
  val ds = DataStoreFinder.getDataStore(inParams).asInstanceOf[AccumuloDataStore]
  val outds = DataStoreFinder.getDataStore(outParams).asInstanceOf[AccumuloDataStore]
  outds.createSchema(trackSft)

  // Construct a query of all features (INCLUDE)
  val q = new Query(pointTypeName)

  def attrToString(attr: Any): String = attr match {
    case null => ""
    case s: String => s
    case d: Date => ISODateTimeFormat.basicDateTime().withZoneUTC().print(d.getTime)
    case g: Geometry => WKTUtils.write(g)
    case a => a.toString
  }

  def main(args: Array[String]) {

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
      val idExp = ff.property(trackId)
      iter.map { f => (idExp.evaluate(f).asInstanceOf[String], f) }
    }

    // group by track identifier (requires shuffle):
    val g = tidAndFeature.groupByKey()
    // sort within track identifier groups, by date order
    /*    val sorted = g.mapPartitions { kv =>
          kv.map{ i => (i._1, i._2.map{
            f => (f.getAttribute(date), f)
          } )
          }
        }*/

     

    val gf = new GeometryFactory
    // to RDD[(String, LineString, Int)]   .... ( id, linestring geom, and count-of-points )
    val idLinestrings = g.mapPartitions { iter =>
      iter.map { pair =>
        var ls = gf.createLineString(Array[Coordinate]())
        val coords = pair._2.map(f => f.getDefaultGeometry.asInstanceOf[Point].getCoordinate).toArray.distinct
        if (coords.length > 1) {
          ls = gf.createLineString(coords)
        }
        (pair._1, ls, coords.length)
      }
    }

    val idLinestrings2 = idLinestrings.filter { t =>
      t._3 > 1
    }
    val sfts = idLinestrings2.mapPartitions { iter =>
      iter.map { tuple => {
        new ScalaSimpleFeature(tuple._1.toString, trackSft, Array[AnyRef](tuple._1, tuple._2)).asInstanceOf[SimpleFeature]
      }
      }
    }

    // seems good to do, but had some problems? (maybe related to y, x issue)
    // sfts.cache()

    println("\n\nINFO: Created  " + sfts.count + " linestrings from " + pointRDD.count + " points.\n\n")

    sfts.mapPartitions { ss =>
      ss.map(sf => sf.getAttributes().map(attrToString).mkString("\t"))
    }.saveAsTextFile("hdfs://localhost:9000/user/root/" + trackSft.getTypeName +
          ISODateTimeFormat.basicDateTime().withZoneUTC().print(Calendar.getInstance.getTime.getTime))

    GeoMesaSpark.save(sfts, outParams, trackSft.getTypeName)
  }
}


package org.locationtech.geomesa.compute.spark.analytics

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext, Partitioner}
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
    builder.add("dtg", classOf[Date])
    builder.add("endDtg", classOf[Date])
    builder.add("geom", classOf[LineString], 4326)
    builder.buildFeatureType()
  }

  // Get a handle to the data stores
  val ds = DataStoreFinder.getDataStore(inParams).asInstanceOf[AccumuloDataStore]
  val outds = DataStoreFinder.getDataStore(outParams).asInstanceOf[AccumuloDataStore]
  outds.createSchema(trackSft)

  // Construct a query of all features (INCLUDE)
  val q = new Query(pointTypeName)

  // Helpers to enable parition  by track id and sort by date
  case class PointKey(trackId: String, pointDtg: Date)

  object PointKey {
    implicit def orderingByDate[A <: PointKey]: Ordering[A] = {
      Ordering.by(pk => (pk.trackId, pk.pointDtg))
    }
  }

  def createKey(sf: SimpleFeature): PointKey = {
    PointKey(sf.getAttribute(trackId).asInstanceOf[String], sf.getAttribute(date).asInstanceOf[Date])
  }

  def createKeyValueTuple(sf: SimpleFeature): (PointKey, SimpleFeature) = {
    (createKey(sf), sf)
  }

  class PointFeaturePartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[PointKey]
      math.abs(k.trackId.hashCode()) % numPartitions
    }
  }


  def attrToString(attr: Any): String = attr match {
    case null => ""
    case s: String => s
    case d: Date => ISODateTimeFormat.basicDateTime().withZoneUTC().print(d.getTime)
    case g: Geometry => WKTUtils.write(g)
    case a => a.toString
  }

  def main(args: Array[String]) {

    // Configure Spark
    val sc = new SparkContext(GeoMesaSpark.init(new SparkConf(true), ds))

    // Create an RDD of point features, RDD[SimpleFeature]
    val pointRDD = GeoMesaSpark.rdd(new Configuration, sc, inParams, q, None)
    println("\n\nINFO: Read " + pointRDD.count + " features from GeoMesa.\n\n")

    // Create key-value tuple  RDD[(PointKey, SimpleFeature)]
    val keyPointRDD = pointRDD.map(sf => createKeyValueTuple(sf))
    println("\n\nINFO: Applied key to " + keyPointRDD.count() + " tuples.\n\n")

    // Repartition and sort
    val keyedPointsSorted = keyPointRDD.repartitionAndSortWithinPartitions(new PointFeaturePartitioner(pointRDD.partitions.length))
    println("\n\nINFO: Sorted into " + keyedPointsSorted.count + "\n\n")

    val gf = new GeometryFactory

    val foo: org.apache.spark.rdd.RDD[(String, Array[AnyRef])] = keyedPointsSorted.mapPartitions { it: Iterator[(PointKey, SimpleFeature)] =>
      it.map { pr: (PointKey, SimpleFeature) => {
        val dtg = pr._2.getAttribute(date).asInstanceOf[Date]
        val pt = pr._2.getDefaultGeometry.asInstanceOf[Point]
        val ls = Array[Coordinate](pt.getCoordinate)
        // template of our simple feature type target
        val attrs: Array[AnyRef] = Array[AnyRef](pr._1.trackId, dtg, dtg, Array[Coordinate](pt.getCoordinate))

        (pr._1.trackId, attrs)
      }}
    }
    println("\nINFO: Mapped to Array[AnyRef]. Sample of 3: \n")
    foo.take(3).foreach(println)

    val red = foo.reduceByKey { (cd1: Array[AnyRef], cd2: Array[AnyRef]) =>
      val startDate : Date = if(cd1(1).asInstanceOf[Date] before cd2(1).asInstanceOf[Date])
        cd1(1).asInstanceOf[Date] else cd2(1).asInstanceOf[Date]
      val endDate : Date = if(cd1(2).asInstanceOf[Date] before cd2(2).asInstanceOf[Date])
        cd2(2).asInstanceOf[Date] else cd1(2).asInstanceOf[Date]
      val coordArray : Array[Coordinate]  = cd1(3).asInstanceOf[Array[Coordinate]] ++ cd2(3).asInstanceOf[Array[Coordinate]]
      Array[AnyRef](cd1(0).asInstanceOf[String], startDate, endDate, coordArray)
    }

    println("\n\nINFO: Reduced to " + red.count + " prospective tracks.\n\n")

    //filter out where teh coordArray has <= 1 elements
    val redFilter = red.filter { pr: (String, Array[AnyRef]) =>
      pr._2(3).asInstanceOf[Array[Coordinate]].distinct.length > 1
    }

    println("\n\nINFO: Filtered empty and single points. Result is "  + redFilter.count + " track recipes.")

    val sfts = redFilter.mapPartitions{ iter : Iterator[(String, Array[AnyRef])] =>
      iter.map{ pr : (String, Array[AnyRef]) => {
        // check there are >1 distinct points to build linestring.
        val coords : Array[Coordinate] = pr._2(3).asInstanceOf[Array[Coordinate]]
        val ls : LineString =  gf.createLineString(coords)
        val attrs = Array[AnyRef](
          pr._2(0).asInstanceOf[String],
          pr._2(1).asInstanceOf[Date],
          pr._2(2).asInstanceOf[Date],
          ls
        )
        new ScalaSimpleFeature(pr._1.toString, trackSft, attrs).asInstanceOf[SimpleFeature]
      }}
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


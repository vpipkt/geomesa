/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.sql.Timestamp
import java.time.Instant
import java.util.{Date, UUID}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.index.strtree.{AbstractNode, Boundable, STRtree}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GenericRowWithSchema, ScalaUDF}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType, TimestampType}
import org.apache.spark.storage.StorageLevel
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.memory.cqengine.datastore.GeoCQEngineDataStore
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SftArgs, SimpleFeatureTypes}
import org.opengis.feature.`type`._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.apache.spark.sql.sources.Filter
import com.vividsolutions.jts.index.sweepline.{SweepLineIndex, SweepLineInterval, SweepLineOverlapAction}
import org.locationtech.geomesa.utils.text.WKTUtils

import scala.collection.Iterator
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Try

object GeoMesaSparkSQL {
  val GEOMESA_SQL_FEATURE = "geomesa.feature"
}

import org.locationtech.geomesa.spark.GeoMesaSparkSQL._

// Spark DataSource for GeoMesa
// enables loading a GeoMesa DataFrame as
// {{
// val df = spark.read
//   .format("geomesa")
//   .option(GM.instanceIdParam.getName, "mycloud")
//   .option(GM.userParam.getName, "user")
//   .option(GM.passwordParam.getName, "password")
//   .option(GM.tableNameParam.getName, "sparksql")
//   .option(GM.mockParam.getName, "true")
//   .option("geomesa.feature", "chicago")
//   .load()
// }}
class GeoMesaDataSource extends DataSourceRegister
  with RelationProvider with SchemaRelationProvider with CreatableRelationProvider
  with LazyLogging {
  import CaseInsensitiveMapFix._

  override def shortName(): String = "geomesa"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    SQLTypes.init(sqlContext)

    // TODO: Need different ways to retrieve sft
    //  GEOMESA-1643 Add method to lookup SFT to RDD Provider
    //  Below the details of the Converter RDD Provider and Providers which are backed by GT DSes are leaking through
    val ds = DataStoreFinder.getDataStore(parameters)
    val sft = if (ds != null) {
      ds.getSchema(parameters(GEOMESA_SQL_FEATURE))
    } else {
      if (parameters.contains(GEOMESA_SQL_FEATURE) && parameters.contains("geomesa.sft")) {
        SimpleFeatureTypes.createType(parameters(GEOMESA_SQL_FEATURE), parameters("geomesa.sft"))
      } else {
        SftArgResolver.getArg(SftArgs(parameters(GEOMESA_SQL_FEATURE), parameters(GEOMESA_SQL_FEATURE))) match {
          case Right(s) => s
          case Left(e) => throw new IllegalArgumentException("Could not resolve simple feature type", e)
        }

      }
    }
    logger.trace(s"Creating GeoMesa Relation with sft : $sft")

    val schema = sft2StructType(sft)
    GeoMesaRelation(sqlContext, sft, schema, parameters)
  }

  // JNH: Q: Why doesn't this method have the call to SQLTypes.init(sqlContext)?
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val ds = DataStoreFinder.getDataStore(parameters)
    val sft = ds.getSchema(parameters(GEOMESA_SQL_FEATURE))
    GeoMesaRelation(sqlContext, sft, schema, parameters)
  }

  private def sft2StructType(sft: SimpleFeatureType) = {
    val fields = sft.getAttributeDescriptors.flatMap { ad => ad2field(ad) }.toList
    StructType(StructField("__fid__", DataTypes.StringType, nullable =false) :: fields)
  }

  def structType2SFT(struct: StructType, name: String): SimpleFeatureType = {
    import java.{lang => jl}
    val fields = struct.fields

    val builder = new SimpleFeatureTypeBuilder

    fields.filter( _.name != "__fid__").foreach {
      field =>
        field.dataType match {
          case DataTypes.BooleanType => builder.add(field.name, classOf[jl.Boolean])
          case DataTypes.DateType => builder.add(field.name, classOf[java.util.Date])
          case DataTypes.FloatType => builder.add(field.name, classOf[jl.Float])
          case DataTypes.IntegerType => builder.add(field.name, classOf[jl.Integer])
          case DataTypes.DoubleType => builder.add(field.name, classOf[jl.Double])
          case DataTypes.StringType => builder.add(field.name, classOf[jl.String])
          case DataTypes.LongType   => builder.add(field.name, classOf[jl.Long])
          case DataTypes.TimestampType => builder.add(field.name, classOf[java.util.Date])

          case SQLTypes.PointTypeInstance => builder.add(field.name, classOf[com.vividsolutions.jts.geom.Point])
          case SQLTypes.LineStringTypeInstance => builder.add(field.name, classOf[com.vividsolutions.jts.geom.LineString])
          case SQLTypes.PolygonTypeInstance  => builder.add(field.name, classOf[com.vividsolutions.jts.geom.Polygon])
          case SQLTypes.MultipolygonTypeInstance  => builder.add(field.name, classOf[com.vividsolutions.jts.geom.MultiPolygon])
          case SQLTypes.GeometryTypeInstance => builder.add(field.name, classOf[com.vividsolutions.jts.geom.Geometry])
        }
    }
    builder.setName(name)
    builder.buildFeatureType()
  }

  private def ad2field(ad: AttributeDescriptor): Option[StructField] = {
    import java.{lang => jl}
    val dt = ad.getType.getBinding match {
      case t if t == classOf[jl.Double]                       => DataTypes.DoubleType
      case t if t == classOf[jl.Float]                        => DataTypes.FloatType
      case t if t == classOf[jl.Integer]                      => DataTypes.IntegerType
      case t if t == classOf[jl.String]                       => DataTypes.StringType
      case t if t == classOf[jl.Boolean]                      => DataTypes.BooleanType
      case t if t == classOf[jl.Long]                         => DataTypes.LongType
      case t if t == classOf[java.util.Date]                  => DataTypes.TimestampType

      case t if t == classOf[com.vividsolutions.jts.geom.Point]            => SQLTypes.PointTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.MultiPoint]       => SQLTypes.MultiPointTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.LineString]       => SQLTypes.LineStringTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.MultiLineString]  => SQLTypes.MultiLineStringTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.Polygon]          => SQLTypes.PolygonTypeInstance
      case t if t == classOf[com.vividsolutions.jts.geom.MultiPolygon]     => SQLTypes.MultipolygonTypeInstance

      case t if      classOf[Geometry].isAssignableFrom(t)    => SQLTypes.GeometryTypeInstance

      // NB:  List and Map types are not supported.
      case _                                                  => null
    }
    Option(dt).map(StructField(ad.getLocalName, _))
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val newFeatureName = parameters(GEOMESA_SQL_FEATURE)
    val sft: SimpleFeatureType = structType2SFT(data.schema, newFeatureName)

    // reuse the __fid__ if available for joins,
    // otherwise use a random id prefixed with the current time
    val fidIndex = data.schema.fields.indexWhere(_.name == "__fid__")
    val fidFn: Row => String =
      if(fidIndex > -1) (row: Row) => row.getString(fidIndex)
      else _ => "%d%s".format(Instant.now().getEpochSecond, UUID.randomUUID().toString)


    val ds = DataStoreFinder.getDataStore(parameters)
    sft.getUserData.put("override.reserved.words", java.lang.Boolean.TRUE)
    ds.createSchema(sft)

    val structType = if (data.queryExecution == null) {
      sft2StructType(sft)
    } else {
      data.schema
    }

    val rddToSave: RDD[SimpleFeature] = data.rdd.mapPartitions( iterRow => {
      val innerDS = DataStoreFinder.getDataStore(parameters)
      val sft = innerDS.getSchema(newFeatureName)
      val builder = new SimpleFeatureBuilder(sft)

      val nameMappings: List[(String, Int)] = SparkUtils.getSftRowNameMappings(sft, structType)
      iterRow.map { r =>
        SparkUtils.row2Sf(nameMappings, r, builder, fidFn(r))
      }
    })

    GeoMesaSpark(parameters).save(rddToSave, parameters, newFeatureName)

    GeoMesaRelation(sqlContext, sft, data.schema, parameters)
  }
}

// the Spark Relation that builds the scan over the GeoMesa table
// used by the SQL optimization rules to push spatio-temporal predicates into the `filt` variable
case class GeoMesaRelation(sqlContext: SQLContext,
                           sft: SimpleFeatureType,
                           schema: StructType,
                           params: Map[String, String],
                           filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
                           props: Option[Seq[String]] = None,
                           var partitionHints : Seq[Int] = null,
                           var indexRDD: RDD[GeoCQEngineDataStore] = null,
                           var partitionedRDD: RDD[(Int, Iterable[SimpleFeature])] = null,
                           var indexPartRDD: RDD[(Int, GeoCQEngineDataStore)] = null)
  extends BaseRelation with PrunedFilteredScan {

  lazy val isMock = Try(params("useMock").toBoolean).getOrElse(false)

  val cache = Try(params("cache").toBoolean).getOrElse(false)

  val indexId = Try(params("indexId").toBoolean).getOrElse(false)

  val indexGeom = Try(params("indexGeom").toBoolean).getOrElse(false)

  val numPartitions = Try(params("partitions").toInt).getOrElse(4)

  val spatiallyPartition = Try(params("spatial").toBoolean).getOrElse(false)

  val partitionStrategy = Try(params("strategy").toString).getOrElse("EQUAL")

  var partitionEnvelopes: List[Envelope] = null

  val providedBounds = Try(params("bounds").toString).getOrElse(null)

  // Control partitioning strategies that require a sample of the data
  val sampleSize = Try(params("sampleSize").toInt).getOrElse(100)
  val thresholdMultiplier = Try(params("threshold").toDouble).getOrElse(0.3)

  val initialQuery = Try(params("query").toString).getOrElse("INCLUDE")

  lazy val rawRDD: SpatialRDD = buildRawRDD

  def buildRawRDD = {
    val raw = GeoMesaSpark(params).rdd(
      new Configuration(), sqlContext.sparkContext, params,
      new Query(params(GEOMESA_SQL_FEATURE), ECQL.toFilter(initialQuery)))

    if (raw.getNumPartitions != numPartitions && params.contains("partitions")) {
      SpatialRDD(raw.repartition(numPartitions), raw.schema)
    } else {
      raw
    }
  }

  val encodedSFT: String = org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.encodeType(sft, true)

  if (partitionedRDD == null && spatiallyPartition) {
    if (partitionEnvelopes == null) {
      val bounds: Envelope = if (providedBounds == null) {
        RelationUtils.getBound(rawRDD)
      } else {
        WKTUtils.read(providedBounds).getEnvelopeInternal
      }
      partitionEnvelopes = partitionStrategy match {
        case "EARTH" => RelationUtils.wholeEarthPartitioning(numPartitions)
        case "EQUAL" => RelationUtils.equalPartitioning(bounds, numPartitions)
        case "WEIGHTED" => RelationUtils.weightedPartitioning(rawRDD, bounds, numPartitions, sampleSize)
        case "RTREE" => RelationUtils.rtreePartitioning(rawRDD, numPartitions, sampleSize, thresholdMultiplier)
        case _ => throw new IllegalArgumentException(s"Invalid partitioning strategy specified: $partitionStrategy")
      }
    }

    partitionedRDD = RelationUtils.spatiallyPartition(partitionEnvelopes, rawRDD, numPartitions)
    partitionedRDD.persist(StorageLevel.MEMORY_ONLY)
  }

  if (cache) {
    if (spatiallyPartition && indexPartRDD == null) {
      indexPartRDD = RelationUtils.indexPartitioned(encodedSFT, sft.getTypeName, partitionedRDD, indexId, indexGeom)
      partitionedRDD.unpersist() // make this call blocking?
      indexPartRDD.persist(StorageLevel.MEMORY_ONLY)
    } else if (indexRDD == null) {
      indexRDD = RelationUtils.index(encodedSFT, sft.getTypeName, rawRDD, indexId, indexGeom)
      indexRDD.persist(StorageLevel.MEMORY_ONLY)
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    if (cache) {
      if (spatiallyPartition) {
        RelationUtils.buildScanInMemoryPartScan(requiredColumns, filters, filt,
                                              sqlContext.sparkContext, schema, params, partitionHints, indexPartRDD)
      } else {
        RelationUtils.buildScanInMemoryScan(requiredColumns, filters, filt, sqlContext.sparkContext, schema, params, indexRDD)
      }
    } else {
      RelationUtils.buildScan(requiredColumns, filters, filt, sqlContext.sparkContext, schema, params)
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t @ (_:IsNotNull | _:IsNull) => true
      case _ => false
    }
  }
}

// A special case relation that is built when a join happens across two identically partitioned relations
// Uses the sweepline algorithm to lower the complexity of the join
case class GeoMesaJoinRelation(sqlContext: SQLContext,
                           leftRel: GeoMesaRelation,
                           rightRel: GeoMesaRelation,
                           schema: StructType,
                           condition: Expression,
                           filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
                           props: Option[Seq[String]] = None)
  extends BaseRelation with PrunedFilteredScan {

  // Uses sweep-line algorithm to join a spatially partitioned RDD
  def sweeplineJoin(overlapAction: OverlapAction): RDD[(Int, (SimpleFeature, SimpleFeature))] = {
    implicit val ordering = RelationUtils.CoordinateOrdering
    val partitionPairs = leftRel.partitionedRDD.join(rightRel.partitionedRDD)

    partitionPairs.flatMap { case (key, (left, right)) =>
      val sweeplineIndex = new SweepLineIndex()
      left.foreach{feature =>
        val coords = feature.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates
        val interval = new SweepLineInterval(coords.min.x, coords.max.x, (0, feature))
        sweeplineIndex.add(interval)
      }
      right.foreach{feature =>
        val coords = feature.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates
        val interval = new SweepLineInterval(coords.min.x, coords.max.x, (1, feature))
        sweeplineIndex.add(interval)
      }
      sweeplineIndex.computeOverlaps(overlapAction)
      overlapAction.joinList.map{ f => (key, f)}
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    val leftSchema = leftRel.schema
    val rightSchema = rightRel.schema
    val leftExtractors = SparkUtils.getExtractors(leftSchema.fieldNames, leftSchema)
    val rightExtractors = SparkUtils.getExtractors(rightSchema.fieldNames, rightSchema)

    // Extract geometry indexes and spatial function from condition expression and relation SFTs
    val (leftIndex, rightIndex, conditionFunction) = condition match {
      case ScalaUDF(function: ((Geometry, Geometry) => Boolean), _, children: Seq[AttributeReference], _ , _) =>
        // Because the predicate may not have parameters in the right order, we must check both
        val leftAttr = children(0).name
        val rightAttr = children(1).name
        val leftIndex = leftRel.sft.indexOf(leftAttr)
        if (leftIndex == -1) {
          (leftRel.sft.indexOf(rightAttr), rightRel.sft.indexOf(leftAttr), function)
        } else {
          (leftIndex, rightRel.sft.indexOf(rightAttr), function)
        }
    }

    // Perform the sweepline join and build rows containing matching features
    val overlapAction = new OverlapAction(leftIndex, rightIndex, conditionFunction)
    val joinedRows: RDD[(Int, (SimpleFeature, SimpleFeature))] = sweeplineJoin(overlapAction)
    joinedRows.mapPartitions{ iter =>
      val joinedSchema = StructType(leftSchema.fields ++ rightSchema.fields)
      val joinedExtractors = leftExtractors ++ rightExtractors

      iter.map{ case (_, (leftFeature, rightFeature)) =>
          SparkUtils.joinedSf2row(joinedSchema, leftFeature, rightFeature, joinedExtractors)
      }
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t @ (_:IsNotNull | _:IsNull) => true
      case _ => false
    }
  }
}

object RelationUtils extends LazyLogging {
  import CaseInsensitiveMapFix._

  @transient val ff = CommonFactoryFinder.getFilterFactory2

  implicit val CoordinateOrdering: Ordering[Coordinate] = Ordering.by {_.x}

  def indexIterator(sft: SimpleFeatureType, indexId: Boolean, indexGeom: Boolean): GeoCQEngineDataStore = {
    val engineStore = new org.locationtech.geomesa.memory.cqengine.datastore.GeoCQEngineDataStore(indexGeom)
    engineStore.createSchema(sft)
    engineStore
  }

  def index(encodedSft: String, typeName: String, rdd: RDD[SimpleFeature], indexId: Boolean, indexGeom: Boolean): RDD[GeoCQEngineDataStore] = {
    rdd.mapPartitions { iter =>
        val sft = SimpleFeatureTypes.createType(typeName,encodedSft)
        val engineStore = RelationUtils.indexIterator(sft, indexId, indexGeom)
        val engine = engineStore.namesToEngine(typeName)
        engine.addAll(iter.toList)
        Iterator(engineStore)
    }
  }

  def indexPartitioned(encodedSft: String,
                       typeName: String,
                       rdd: RDD[(Int, Iterable[SimpleFeature])],
                       indexId: Boolean,
                       indexGeom: Boolean): RDD[(Int, GeoCQEngineDataStore)] = {
    rdd.mapValues { iter =>
      val sft = SimpleFeatureTypes.createType(typeName,encodedSft)
      val engineStore = RelationUtils.indexIterator(sft, indexId, indexGeom)
      val engine = engineStore.namesToEngine(typeName)
      engine.addAll(iter)
      engineStore
    }
  }

  // Maps a SimpleFeature to the id of the envelope that contains it
  // Will duplicate features that belong to more than one envelope
  // Returns -1 if no match was found
  // TODO: Filter duplicates when querying
  def gridIdMapper(sf: SimpleFeature, envelopes: List[Envelope]): List[(Int, SimpleFeature)] = {
    val geom = sf.getDefaultGeometry.asInstanceOf[Geometry]
    val mappings = envelopes.indices.flatMap { index =>
      if (envelopes(index).intersects(geom.getEnvelopeInternal)) {
        Some(index, sf)
      } else {
        None
      }
    }
    if (mappings.isEmpty) {
      List((-1, sf))
    } else {
      mappings.toList
    }
  }

  // Maps a geometry to the id of the envelope that contains it
  // Used to derive partition hints
  def gridIdMapper(geom: Geometry, envelopes: List[Envelope]): List[Int] = {
    val mappings = envelopes.indices.flatMap { index =>
      if (envelopes(index).intersects(geom.getEnvelopeInternal)) {
        Some(index)
      } else {
        None
      }
    }
    if (mappings.isEmpty) {
      List(-1)
    } else {
      mappings.toList
    }
  }

  def spatiallyPartition(envelopes: List[Envelope], rdd: RDD[SimpleFeature], numPartitions: Int): RDD[(Int, Iterable[SimpleFeature])] = {
    val keyedRdd = rdd.flatMap { gridIdMapper( _, envelopes)}
    val partitioned = keyedRdd.groupByKey(new IndexPartitioner(numPartitions))
    partitioned.foreachPartition{ iter =>
      iter.foreach{ case (key, features) =>
        logger.info(s"partition $key has ${features.size} features")
      }
    }
    partitioned
  }

  def getBound(rdd: RDD[SimpleFeature]): Envelope = {
    rdd.aggregate[Envelope](new Envelope())(
      (env: Envelope, sf: SimpleFeature) => {
        env.expandToInclude(sf.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal)
        env
      },
      (env1: Envelope, env2: Envelope) => {
        env1.expandToInclude(env2)
        env1
      }
    )
  }

  def equalPartitioning(bound: Envelope, numPartitions: Int): List[Envelope] = {
    // Compute bounds of each partition
    val partitionsPerDim = Math.sqrt(numPartitions).toInt
    val partitionWidth = bound.getWidth / partitionsPerDim
    val partitionHeight = bound.getHeight / partitionsPerDim
    val minX = bound.getMinX
    val minY = bound.getMinY
    val partitionEnvelopes: ListBuffer[Envelope] = ListBuffer()

    // Build partitions
    for (xIndex <- 0 until partitionsPerDim) {
      val xPartitionStart = minX + (xIndex * partitionWidth)
      val xPartitionEnd = xPartitionStart + partitionWidth
      for (yIndex <- 0 until partitionsPerDim) {
        val yPartitionStart = minY + (yIndex * partitionHeight)
        val yPartitionEnd = yPartitionStart+ partitionHeight
        partitionEnvelopes += new Envelope(xPartitionStart, xPartitionEnd, yPartitionStart, yPartitionEnd)
      }
    }
    partitionEnvelopes.toList
  }

  def weightedPartitioning(rawRDD: RDD[SimpleFeature], bound: Envelope, numPartitions: Int, sampleSize: Int): List[Envelope] = {
    val width: Int = Math.sqrt(numPartitions).toInt
    val binSize = sampleSize / width
    val sample = rawRDD.takeSample(withReplacement = false, sampleSize)
    val xSample = sample.map{f => f.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates.min.x}
    val ySample = sample.map{f => f.getDefaultGeometry.asInstanceOf[Geometry].getCoordinates.min.y}
    val xSorted = xSample.sorted
    val ySorted = ySample.sorted

    val partitionEnvelopes: ListBuffer[Envelope] = ListBuffer()

    for (xBin <- 0 until width) {
      val minX = xSorted(xBin * binSize)
      val maxX = xSorted(((xBin + 1) * binSize) - 1)
      for (yBin <- 0 until width) {
        val minY = ySorted(yBin)
        val maxY = ySorted(((yBin + 1) * binSize) - 1)
        partitionEnvelopes += new Envelope(minX, maxX, minY, maxY)
      }
    }

    partitionEnvelopes.toList
  }

  def wholeEarthPartitioning(numPartitions: Int): List[Envelope] = {
    equalPartitioning(new Envelope(-180,180,-90,90), numPartitions)
  }

  // Constructs an RTree based on a sample of the data and returns its bounds as envelopes
  // returns one less envelope than requested to account for the catch-all envelope
  def rtreePartitioning(rawRDD: RDD[SimpleFeature], numPartitions: Int, sampleSize: Int, thresholdMultiplier: Double): List[Envelope] = {
    val sample = rawRDD.takeSample(withReplacement = false, sampleSize)
    val rtree = new STRtree()

    sample.foreach{ sf =>
      rtree.insert(sf.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal, sf)
    }
    val envelopes: java.util.List[Envelope] = new java.util.ArrayList[Envelope]()

    // get rtree envelopes, limited to those containing reasonable size
    val reasonableSize = sampleSize / numPartitions
    val threshold = (reasonableSize * thresholdMultiplier).toInt
    val minSize = reasonableSize - threshold
    val maxSize = reasonableSize + threshold
    rtree.build()
    queryBoundary(rtree.getRoot, envelopes, minSize, maxSize)
    envelopes.take(numPartitions-1).toList
  }

  // Helper method to get the envelopes of an RTree
  def queryBoundary(node: AbstractNode, boundaries: java.util.List[Envelope], minSize: Int, maxSize: Int): Int =  {
    // get node's immediate children
    val childBoundables: java.util.List[_] = node.getChildBoundables

    // True if current node is leaf
    var flagLeafnode = true
    var i = 0
    while (i < childBoundables.size && flagLeafnode) {
      val childBoundable = childBoundables.get(i).asInstanceOf[Boundable]
      if (childBoundable.isInstanceOf[AbstractNode]) {
        flagLeafnode = false
      }
      i += 1
    }

    if (flagLeafnode) {
      childBoundables.size
    } else {
      var nodeCount = 0
      for ( i <- 0 until childBoundables.size ) {
        val childBoundable = childBoundables.get(i).asInstanceOf[Boundable]
        childBoundable match {
          case (child: AbstractNode) =>
            val childSize = queryBoundary(child, boundaries, minSize, maxSize)
            // check boundary for size and existence in chosen boundaries
            if (childSize < maxSize && childSize > minSize) {
              var alreadyAdded = false
              if (node.getLevel != 1) {
                child.getChildBoundables.asInstanceOf[java.util.List[AbstractNode]].foreach { c =>
                  alreadyAdded = alreadyAdded || boundaries.contains(c.getBounds.asInstanceOf[Envelope])
                }
              }
              if (!alreadyAdded) {
                boundaries.add(child.getBounds.asInstanceOf[Envelope])
              }
            }
            nodeCount += childSize
          case (_) => nodeCount += 1 // negligible difference but accurate
        }
      }
      nodeCount
    }
  }

  def coverPartitioning(dataRDD: RDD[SimpleFeature], coverRDD: RDD[SimpleFeature], numPartitions: Int): List[Envelope] = {
    coverRDD.map {
      _.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    }.collect().toList
  }

  def buildScan(requiredColumns: Array[String],
                filters: Array[org.apache.spark.sql.sources.Filter],
                filt: org.opengis.filter.Filter,
                ctx: SparkContext,
                schema: StructType,
                params: Map[String, String]): RDD[Row] = {
    logger.debug(
      s"""Building scan, filt = $filt,
         |filters = ${filters.mkString(",")},
         |requiredColumns = ${requiredColumns.mkString(",")}""".stripMargin)
    val compiledCQL = filters.flatMap(SparkUtils.sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => ff.and(l, r) }
    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")
    val rdd = GeoMesaSpark(params).rdd(
      new Configuration(), ctx, params,
      new Query(params(GEOMESA_SQL_FEATURE), compiledCQL, requiredAttributes))

    val extractors = SparkUtils.getExtractors(requiredColumns, schema)
    val result = rdd.map(SparkUtils.sf2row(schema, _, extractors))
    result.asInstanceOf[RDD[Row]]
  }

  def buildScanInMemoryScan(requiredColumns: Array[String],
                            filters: Array[org.apache.spark.sql.sources.Filter],
                            filt: org.opengis.filter.Filter,
                            ctx: SparkContext,
                            schema: StructType,
                            params: Map[String, String],  indexRDD: RDD[GeoCQEngineDataStore]): RDD[Row] = {
    logger.debug(
      s"""Building in-memory scan, filt = $filt,
         |filters = ${filters.mkString(",")},
         |requiredColumns = ${requiredColumns.mkString(",")}""".stripMargin)
    val compiledCQL = filters.flatMap(SparkUtils.sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => SparkUtils.ff.and(l, r) }
    val filterString = ECQL.toCQL(compiledCQL)

    val extractors = SparkUtils.getExtractors(requiredColumns, schema)

    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureReader

    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")
    val result = indexRDD.flatMap { engine =>
      val cqlFilter = ECQL.toFilter(filterString)
      val query = new Query(params(GEOMESA_SQL_FEATURE), cqlFilter, requiredAttributes)
      val fr = engine.getFeatureReader(query, Transaction.AUTO_COMMIT)
      fr.toIterator
    }.map(SparkUtils.sf2row(schema, _, extractors))

    result.asInstanceOf[RDD[Row]]
  }

  def buildScanInMemoryPartScan(requiredColumns: Array[String],
                                filters: Array[org.apache.spark.sql.sources.Filter],
                                filt: org.opengis.filter.Filter,
                                ctx: SparkContext,
                                schema: StructType,
                                params: Map[String, String],
                                partitionHints: Seq[Int],
                                indexPartRDD: RDD[(Int, GeoCQEngineDataStore)]): RDD[Row] = {
    logger.debug(
      s"""Building partitioned in-memory scan, filt = $filt,
         |filters = ${filters.mkString(",")},
         |requiredColumns = ${requiredColumns.mkString(",")}""".stripMargin)
    val compiledCQL = filters.flatMap(SparkUtils.sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => SparkUtils.ff.and(l,r) }
    val filterString = ECQL.toCQL(compiledCQL)

    // If keys were derived from query, go straight to those partitions
    val reducedRdd =  if (partitionHints != null) {
      indexPartRDD.filter {case (key, _) => partitionHints.contains(key) }
    } else {
      indexPartRDD
    }

    val extractors = SparkUtils.getExtractors(requiredColumns, schema)

    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureReader

    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")
    val result = reducedRdd.flatMap { case (key, engine) =>
      val cqlFilter = ECQL.toFilter(filterString)
      val query = new Query(params(GEOMESA_SQL_FEATURE), cqlFilter, requiredAttributes)
      val fr = engine.getFeatureReader(query, Transaction.AUTO_COMMIT)
      fr.toIterator
    }.map(SparkUtils.sf2row(schema, _, extractors))
    result.asInstanceOf[RDD[Row]]
  }
}

object SparkUtils {

  @transient val ff = CommonFactoryFinder.getFilterFactory2

  // the SFT attributes do not have the __fid__ so we have to translate accordingly
  def getExtractors(requiredColumns: Array[String], schema: StructType): Array[SimpleFeature => AnyRef] = {
    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")

    type EXTRACTOR = SimpleFeature => AnyRef
    val IdExtractor: SimpleFeature => AnyRef = sf => sf.getID

    requiredColumns.map {
      case "__fid__" => IdExtractor
      case col       =>
        val index = requiredAttributes.indexOf(col)
        val schemaIndex = schema.fieldIndex(col)
        val fieldType = schema.fields(schemaIndex).dataType
        sf: SimpleFeature =>
          if ( fieldType == TimestampType ) {
            new Timestamp(sf.getAttribute(index).asInstanceOf[Date].getTime)
          } else {
            sf.getAttribute(index)
          }
    }
  }


  def sparkFilterToCQLFilter(filt: org.apache.spark.sql.sources.Filter): Option[org.opengis.filter.Filter] = filt match {
    case GreaterThanOrEqual(attribute, v) => Some(ff.greaterOrEqual(ff.property(attribute), ff.literal(v)))
    case GreaterThan(attr, v)             => Some(ff.greater(ff.property(attr), ff.literal(v)))
    case LessThanOrEqual(attr, v)         => Some(ff.lessOrEqual(ff.property(attr), ff.literal(v)))
    case LessThan(attr, v)                => Some(ff.less(ff.property(attr), ff.literal(v)))
    case EqualTo(attr, v) if attr == "__fid__" => Some(ff.id(ff.featureId(v.toString)))
    case EqualTo(attr, v)                      => Some(ff.equals(ff.property(attr), ff.literal(v)))
    case In(attr, values) if attr == "__fid__" => Some(ff.id(values.map(v => ff.featureId(v.toString)).toSet))
    case In(attr, values)                      =>
      Some(values.map(v => ff.equals(ff.property(attr), ff.literal(v))).reduce[org.opengis.filter.Filter]( (l,r) => ff.or(l,r)))
    case And(left, right)                 => Some(ff.and(sparkFilterToCQLFilter(left).get, sparkFilterToCQLFilter(right).get)) // TODO: can these be null
    case Or(left, right)                  => Some(ff.or(sparkFilterToCQLFilter(left).get, sparkFilterToCQLFilter(right).get))
    case Not(f)                           => Some(ff.not(sparkFilterToCQLFilter(f).get))
    case StringStartsWith(a, v)           => Some(ff.like(ff.property(a), s"$v%"))
    case StringEndsWith(a, v)             => Some(ff.like(ff.property(a), s"%$v"))
    case StringContains(a, v)             => Some(ff.like(ff.property(a), s"%$v%"))
    case IsNull(attr)                     => None
    case IsNotNull(attr)                  => None
  }

  def sf2row(schema: StructType, sf: SimpleFeature, extractors: Array[SimpleFeature => AnyRef]): Row = {
    val res = Array.ofDim[Any](extractors.length)
    var i = 0
    while(i < extractors.length) {
      res(i) = extractors(i)(sf)
      i += 1
    }
    new GenericRowWithSchema(res, schema)
  }

  def joinedSf2row(schema: StructType, sf1: SimpleFeature, sf2: SimpleFeature, extractors: Array[SimpleFeature => AnyRef]): Row = {
    val leftLength = sf1.getAttributeCount + 1
    val res = Array.ofDim[Any](extractors.length)
    var i = 0
    while(i < leftLength) {
      res(i) = extractors(i)(sf1)
      i += 1
    }
    while(i < extractors.length) {
      res(i) = extractors(i)(sf2)
      i += 1
    }
    new GenericRowWithSchema(res, schema)
  }

  // Since each attribute's corresponding index in the Row is fixed. Compute the mapping once
  def getSftRowNameMappings(sft: SimpleFeatureType, schema: StructType): List[(String, Int)] = {
    sft.getAttributeDescriptors.map{ ad =>
      val name = ad.getLocalName
      (name, schema.fieldIndex(ad.getLocalName))
    }.toList
  }

  def row2Sf(nameMappings: List[(String, Int)], row: Row, builder: SimpleFeatureBuilder, id: String): SimpleFeature = {
    builder.reset()
    nameMappings.foreach{ case (name, index) =>
      builder.set(name, row.getAs[Object](index))
    }

    builder.userData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    builder.buildFeature(id)
  }
}

class OverlapAction(leftIndex: Int,
                    rightIndex: Int,
                    conditionFunction: (Geometry, Geometry) => Boolean) extends SweepLineOverlapAction with Serializable {

  val joinList = ListBuffer[(SimpleFeature, SimpleFeature)]()

  override def overlap(s0: SweepLineInterval, s1: SweepLineInterval): Unit = {
    val (key0, feature0) = s0.getItem.asInstanceOf[(Int, SimpleFeature)]
    val (key1, feature1) = s1.getItem.asInstanceOf[(Int, SimpleFeature)]
    if (key0 == 0 && key1 == 1) {
      val leftGeom = feature0.getAttribute(leftIndex).asInstanceOf[Geometry]
      val rightGeom = feature1.getAttribute(rightIndex).asInstanceOf[Geometry]
      if (conditionFunction(leftGeom, rightGeom)) {
        joinList.append((feature0, feature1))
      }
    } else if (key0 == 1 && key1 == 0) {
      val leftGeom = feature1.getAttribute(leftIndex).asInstanceOf[Geometry]
      val rightGeom = feature0.getAttribute(rightIndex).asInstanceOf[Geometry]
      if (conditionFunction(leftGeom, rightGeom)) {
        joinList.append((feature1, feature0))
      }
    }

  }
}

/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index.strategies

import java.util.Map.Entry

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection}
import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting}
import org.apache.accumulo.core.data.{Key, Range => AccRange, Value}
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.GEOMESA_ITERATORS_IS_DENSITY_TYPE
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.core.util.{SelfClosingBatchScanner, SelfClosingIterator}
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.expression.Literal
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator}

import scala.util.Try

abstract class BaseSpatioTemporalStrategy extends Strategy with Logging with IndexFilterHelpers {

  import org.locationtech.geomesa.core.index.strategies.BaseSpatioTemporalStrategy._
  import org.locationtech.geomesa.core.index.strategies.Strategy._

  def tryScanner(query: Query,
                 featureType: SimpleFeatureType,
                 indexSchema: String,
                 featureEncoding: FeatureEncoding,
                 bs: BatchScanner,
                 output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val tryScanner = Try {
      val qp = buildQueryPlan(query, featureType, indexSchema, featureEncoding, output)
      configureBatchScanner(bs, qp)
      // NB: Since we are (potentially) gluing multiple batch scanner iterators together,
      //  we wrap our calls in a SelfClosingBatchScanner.
      SelfClosingBatchScanner(bs)
    }
    val scanner = tryScanner.recover {
      case e: Exception =>
        logger.error("Error in creating scanner:", e)
        // since GeoTools would eat the error and return no records anyway,
        // there's no harm in returning an empty iterator.
        SelfClosingIterator[Entry[Key, Value]](Iterator.empty)
    }
    scanner.get
  }

  def buildQueryPlan(query: Query,
                     featureType: SimpleFeatureType,
                     indexSchema: String,
                     featureEncoding: FeatureEncoding,
                     output: ExplainerOutputType): QueryPlan = {
    val keyPlanner      = IndexSchema.buildKeyPlanner(indexSchema)
    val cfPlanner       = IndexSchema.buildColumnFamilyPlanner(indexSchema)

    output(s"Scanning ST index table for feature type ${featureType.getTypeName}")
    output(s"Filter: ${query.getFilter}")

    val dtgField = getDtgFieldName(featureType)

    val (geomFilters, otherFilters) = partitionGeom(query.getFilter, featureType)
    val (temporalFilters, ecqlFilters) = partitionTemporal(otherFilters, dtgField)

    val ecql = filterListAsAnd(ecqlFilters).map(ECQL.toCQL)

    output(s"Geometry filters: $geomFilters")
    output(s"Temporal filters: $temporalFilters")
    output(s"Other filters: $ecqlFilters")

    val (tweakedGeoms, geometryToCover) = getGeometryToCover(geomFilters, featureType)

    output(s"Tweaked geom filters are $tweakedGeoms")
    output(s"GeomToCover: $geometryToCover")

    val timeInterval = netInterval(extractTemporal(dtgField)(temporalFilters))
    val filter = buildFilter(geometryToCover, timeInterval)

    val stFilter = filterListAsAnd(tweakedGeoms ++ temporalFilters)
    if (stFilter.isEmpty) {
      logger.warn(s"Querying Accumulo without SpatioTemporal filter.")
    }

    output(s"STII Filter: ${stFilter.getOrElse("No STII Filter")}")
    output(s"Time Interval:  ${IndexSchema.somewhen(timeInterval).getOrElse("No time interval")}")
    output(s"KeyPlanningFilter: ${Option(filter).getOrElse("No Filter")}")

    val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, featureType)

    val stiiIterCfg = getSTIIIterCfg(iteratorConfig, query, featureType, stFilter, ecql, featureEncoding)
    val densityIterCfg = getDensityIterCfg(query, geometryToCover, indexSchema, featureEncoding, featureType)
    val otherCfgs = getOtherIteratorConfigs(query, featureType, featureEncoding, indexSchema, output)
    val iters = List(Some(stiiIterCfg), densityIterCfg).flatten ++ otherCfgs

    // set up row ranges
    val (ranges, cfs) = getRanges(filter, iteratorConfig.iterator, output, keyPlanner, cfPlanner)

    QueryPlan(iters, ranges, cfs)
  }

  /**
   * Should be implemented by subclasses as needed
   */
  def getOtherIteratorConfigs(query: Query,
                              sft: SimpleFeatureType,
                              encoding: FeatureEncoding,
                              schema: String,
                              output: ExplainerOutputType): List[IteratorSetting] = List.empty

  /**
   * Gets an iterator setting for the main STII iterator
   */
  def getSTIIIterCfg(iteratorConfig: IteratorConfig,
                     query: Query,
                     featureType: SimpleFeatureType,
                     stFilter: Option[Filter],
                     ecqlFilter: Option[String],
                     featureEncoding: FeatureEncoding): IteratorSetting = {
    iteratorConfig.iterator match {
      case IndexOnlyIterator =>
        val transformsCover = iteratorConfig.transformCoversFilter
        configureIndexIterator(featureType, query, featureEncoding, stFilter, transformsCover)
      case SpatioTemporalIterator =>
        val isDensity = query.getHints.containsKey(DENSITY_KEY)
        configureSTII(featureType, query, featureEncoding, stFilter, ecqlFilter, isDensity)
    }
  }

  // returns an iterator over [key,value] pairs where the key is taken from the index row and the value is a SimpleFeature,
  // which is either read directory from the data row  value or generated from the encoded index row value
  // -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureIndexIterator(featureType: SimpleFeatureType,
                             query: Query,
                             featureEncoding: FeatureEncoding,
                             filter: Option[Filter],
                             transformsCoverFilter: Boolean): IteratorSetting = {

    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator, classOf[IndexIterator])

    configureStFilter(cfg, filter)
    if (transformsCoverFilter) {
      // apply the transform directly to the index iterator
      val testType = query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType]
      configureFeatureType(cfg, testType)
    } else {
      // we need to evaluate the original feature before transforming
      // transforms are applied afterwards
      configureFeatureType(cfg, featureType)
      configureTransforms(cfg, query)
    }
    configureIndexValues(cfg, featureType)
    configureFeatureEncoding(cfg, featureEncoding)
    cfg
  }

  // returns only the data entries -- no index entries -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureSTII(featureType: SimpleFeatureType,
                    query: Query,
                    featureEncoding: FeatureEncoding,
                    stFilter: Option[Filter],
                    ecqlFilter: Option[String],
                    isDensity: Boolean): IteratorSetting = {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      classOf[SpatioTemporalIntersectingIterator])
    configureStFilter(cfg, stFilter)
    configureFeatureType(cfg, featureType)
    configureFeatureEncoding(cfg, featureEncoding)
    configureTransforms(cfg, query)
    configureEcqlFilter(cfg, ecqlFilter)
    if (isDensity) {
      cfg.addOption(GEOMESA_ITERATORS_IS_DENSITY_TYPE, "isDensity")
    }
    cfg
  }

  /**
   * Gets accumulo ranges and column families based on the key plan
   */
  def getRanges(filter: KeyPlanningFilter,
                iter: IteratorChoice,
                output: ExplainerOutputType,
                keyPlanner: KeyPlanner,
                cfPlanner: ColumnFamilyPlanner): (Seq[AccRange], Seq[Text]) = {
    output(s"Planning query")

    val indexOnly = iter match {
      case IndexOnlyIterator      => true
      case SpatioTemporalIterator => false
    }

    val keyPlan = keyPlanner.getKeyPlan(filter, indexOnly, output)
    val columnFamilies = cfPlanner.getColumnFamiliesToFetch(filter)

    // always try to use range(s) to remove easy false-positives
    val accRanges: Seq[org.apache.accumulo.core.data.Range] = keyPlan match {
      case KeyRanges(ranges) => ranges.map(r => new AccRange(r.start, r.end))
      case _ => Seq(new AccRange())
    }

    output(s"Total ranges: ${accRanges.size} - ${accRanges.take(5)}")

    // if you have a list of distinct column-family entries, fetch them
    val cf = columnFamilies match {
      case KeyList(keys) =>
        output(s"ColumnFamily Planner: ${keys.size} : ${keys.take(20)}")
        keys.map(cf => new Text(cf))

      case _ => Seq()
    }

    (accRanges, cf)
  }
}

object BaseSpatioTemporalStrategy extends IndexFilterHelpers  {

  /**
   * Does two things:
   *
   * 1. Updates the filters to account for things like international date line, invalid bounding boxes, etc.
   * 2. Calculates a geometry that covers all the filters.
   *
   * @param geometryFilters
   * @param sft
   * @return
   */
  def getGeometryToCover(geometryFilters: Seq[Filter], sft: SimpleFeatureType): (Seq[Filter], Geometry) = {
    val tweakedGeoms = geometryFilters.map(updateTopologicalFilters(_, sft))

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = tweakedGeoms.flatMap {
      case bbox: BBOX =>
        val bboxPoly = bbox.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])
        Seq(bboxPoly)
      case gf: BinarySpatialOperator =>
        extractGeometry(gf)
      case _ => Seq()
    }

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    val geometryToCover = netGeom(collectionToCover)

    (tweakedGeoms, geometryToCover)
  }
}

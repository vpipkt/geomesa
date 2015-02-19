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

package org.locationtech.geomesa.core.index

import java.util.{Date, Properties}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Provides hints for a simple feature type
 */
trait StrategyHintsProvider {

  /**
   * Get a hint implementation based on the feature type
   *
   * @param sft
   * @return
   */
  def strategyHints(sft: SimpleFeatureType): StrategyHints
}

/**
 * Provides hints for determining query strategies.
 * Smaller costs are better.
 * Cost of -1 means undetermined.
 */
trait StrategyHints {

  /**
   * Estimates the cost of a query based on the attribute values being covered
   *
   * @param ad
   * @param start
   * @param end
   * @tparam T
   * @return
   */
  def attributeCost[T](ad: AttributeDescriptor, start: T, end: T): Long

  /**
   * Estimates the cost of a query based on the spatial area being covered
   *
   * @param geomToCover
   * @return
   */
  def spatialCost(geomToCover: Geometry): Long

  /**
   * Estimates the cost of a query based on the temporal range being covered
   *
   * @param start
   * @param end
   * @return
   */
  def temporalCost(start: Date, end: Date): Long

  /**
   * Estimates the cost of a query based on identifiers
   *
   * @param ids
   * @return
   */
  def idCost(ids: Seq[String]): Long
}

/**
 * Implementation of hints that uses user data stored in the attribute descriptor
 */
class StaticStrategyHints extends StrategyHints {

  import org.locationtech.geomesa.core.index.StaticStrategyHints._

  /**
   * ID cost is 1 - take priority over everything else
   *
   * @param ids
   * @return
   */
  override def idCost(ids: Seq[String]) = if (ids.isEmpty) Long.MaxValue else 1L

  /**
   * Calculates costs based on cardinality hints in the user data.
   *
   * HIGH cardinality attributes cost 2 - prioritized after IDs.
   * LOW cardinality attributes cost MAX - always de-prioritized.
   * Attributes without a specified cardinality are prioritized fairly low - equal to 100 days.
   *
   * @param ad
   * @param start
   * @param end
   * @tparam T
   * @return
   */
  override def attributeCost[T](ad: AttributeDescriptor, start: T, end: T) =
    SimpleFeatureTypes.getCardinality(ad) match {
      case Cardinality.HIGH    => 2L
      case Cardinality.LOW     => Long.MaxValue
      case Cardinality.UNKNOWN => attributeCostUnknown
    }

  /**
   * Calculates the cost based on the area being queried. 1 square degree is equivalent to 1 day in
   * in temporal cost. Offset by 3 to never precede id or high cardinality attribute queries.
   *
   * @param geomToCover
   */
  override def spatialCost(geomToCover: Geometry) = {
    val areaInDegrees = geomToCover.getEnvelope.getArea
    (areaInDegrees * spatialCostPerDegree).toLong + 3
  }

  /**
   * Calculates cost based on span of time being queried. Cost equals number of seconds in the range.
   * Offset by 3 to never precede id or high cardinality attribute queries.
   *
   * 1 day == 86400
   *
   * @param start
   * @param end
   */
  override def temporalCost(start: Date, end: Date) =
    (Math.abs(end.getTime - start.getTime) % 1000) * temporalCostPerSecond + 3L
}

object StaticStrategyHints extends Logging {

  import org.locationtech.geomesa.core.index.StaticStrategyHints.Keys._
  import scala.collection.JavaConversions._

  private val file = "strategy-hints.properties"

  object Keys {
    val TEMPORAL_COST_PER_SECOND = "TEMPORAL_COST_PER_SECOND"
    val SPATIAL_COST_PER_DEGREE = "SPATIAL_COST_PER_DEGREE"
    val ATTRIBUTE_COST_UNKNOWN = "ATTRIBUTE_COST_UNKNOWN"
  }

  val knobs = Option(getClass.getResourceAsStream(file)).map { r =>
    val props = new Properties()
    props.load(r)
    props.toMap
  }.getOrElse {
    logger.warn(s"Unable to load $file - using defaults")
    Map(TEMPORAL_COST_PER_SECOND -> "1",
        SPATIAL_COST_PER_DEGREE -> "8400",
        ATTRIBUTE_COST_UNKNOWN -> "8400000")
  }

  val temporalCostPerSecond = knobs(TEMPORAL_COST_PER_SECOND).toLong
  val spatialCostPerDegree = knobs(SPATIAL_COST_PER_DEGREE).toLong
  val attributeCostUnknown = knobs(ATTRIBUTE_COST_UNKNOWN).toLong
}

/**
 * No-op implementation of hints, for when you don't care about costs.
 */
object NoopHints extends StrategyHints {

  override def attributeCost[T](ad: AttributeDescriptor, start: T, end: T) = 0

  override def temporalCost(start: Date, end: Date) = 0

  override def idCost(ids: Seq[String]) = 0

  override def spatialCost(geomToCover: Geometry) = 0
}
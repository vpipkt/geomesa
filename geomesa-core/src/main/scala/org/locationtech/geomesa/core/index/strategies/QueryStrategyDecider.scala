/*
 * Copyright 2014-2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index.strategies

import org.geotools.data.Query
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryHints._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, PropertyIsLike}

import scala.collection.JavaConversions._

object QueryStrategyDecider {

  def chooseStrategy(sft: SimpleFeatureType, query: Query, hints: StrategyHints, version: Int): Strategy = {

    val isDensity = query.getHints.containsKey(BBOX_KEY) || query.getHints.contains(TIME_BUCKETS_KEY)
    if (isDensity) {
      // TODO GEOMESA-322 use other strategies with density iterator
      return new STIdxStrategy
    }

    val filter = query.getFilter

    // check for simple filters first
    // order - id, attribute, temporal
    val strategy = RecordIdxStrategy.getStrategy(filter, sft)
        .orElse(AttributeIndexStrategy.getStrategy(filter, sft))
        // TODO re-enable this
        // .orElse(if (version < 3) None else TimeIndexStrategy.getStrategy(filter, sft))
        .map(_.strategy)

    strategy.getOrElse {
      filter match {
        case and: And     => chooseAndStrategy(and, sft, hints, version)
        case cql          => new STIdxStrategy // default
      }
    }
  }

  /**
   * Choose the query strategy to be employed here. This is based off the estimated cost of each
   * filter, derived from the StrategyHints.
   *
   * @param and
   * @param sft
   * @param hints
   * @param version
   * @return
   */
  private def chooseAndStrategy(and: And,
                                sft: SimpleFeatureType,
                                hints: StrategyHints,
                                version: Int): Strategy = {

    val filters = decomposeAnd(and)

    // TODO we could combine filters on the same attribute to get a better cost estimate
    val recordStrategies = filters.flatMap(RecordIdxStrategy.getStrategy(_, sft, hints))
    val spatialStrategies = filters.flatMap(STIdxStrategy.getStrategy(_, sft, hints))
    val attributeStrategies = filters.flatMap(AttributeIndexStrategy.getStrategy(_, sft, hints))
    val temporalStrategies = if (version < 3) {
      Seq.empty
    } else {
      // TODO re-enable this
      // filters.flatMap(TimeIndexStrategy.getStrategy(_, sft, hints))
      Seq.empty
    }

    val strategies = recordStrategies ++ spatialStrategies ++ attributeStrategies ++ temporalStrategies
    val bestStrategy = strategies.reduceOption(findCheapestStrategy).map(_.strategy)

    bestStrategy.getOrElse(new STIdxStrategy)
  }

  /**
   * Checks cost of strategies. In case of tie, order in the query takes precedence.
   *
   * @return
   */
  private def findCheapestStrategy =
    (s1: StrategyDecision, s2: StrategyDecision) => if (s2.cost < s1.cost) s2 else s1

  // TODO try to use wildcard values from the Filter itself (https://geomesa.atlassian.net/browse/GEOMESA-309)
  // Currently pulling the wildcard values from the filter
  // leads to inconsistent results...so use % as wildcard
  val MULTICHAR_WILDCARD = "%"
  val SINGLE_CHAR_WILDCARD = "_"
  val NULLBYTE = Array[Byte](0.toByte)

  /* Like queries that can be handled by current reverse index */
  def likeEligible(filter: PropertyIsLike) = containsNoSingles(filter) && trailingOnlyWildcard(filter)

  /* contains no single character wildcards */
  def containsNoSingles(filter: PropertyIsLike) =
    !filter.getLiteral.replace("\\\\", "").replace(s"\\$SINGLE_CHAR_WILDCARD", "").contains(SINGLE_CHAR_WILDCARD)

  def trailingOnlyWildcard(filter: PropertyIsLike) =
    (filter.getLiteral.endsWith(MULTICHAR_WILDCARD) &&
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == filter.getLiteral.length - MULTICHAR_WILDCARD.length) ||
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == -1

}

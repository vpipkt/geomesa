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

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.temporal.`object`.DefaultPeriod
import org.locationtech.geomesa.core
import org.locationtech.geomesa.core.data.AccumuloConnectorCreator
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.index.strategies.Strategy._
import org.locationtech.geomesa.core.iterators.{RowSkippingIterator, IndexIterator, IteratorChoice}
import org.locationtech.geomesa.feature.FeatureEncoding._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.temporal.{After, Before, During, TEquals}

class TimeIndexStrategy extends BaseSpatioTemporalStrategy {

  override def execute(query: Query,
                       featureType: SimpleFeatureType,
                       indexSchema: String,
                       featureEncoding: FeatureEncoding,
                       acc: AccumuloConnectorCreator,
                       output: ExplainerOutputType) = {
    val bs = acc.createTimeIndexScanner(featureType)
    tryScanner(query, featureType, indexSchema, featureEncoding, bs, output)
  }

  // if there are any parts of the row key after the date, we tweak them here to just cut off the
  // start/end of the range - other rows will be skipped by iterators
  override def adaptKeyPlanner(keyPlanner: KeyPlanner) = keyPlanner match {
    case CompositePlanner(planners, sep) =>
      val i = planners.indexWhere(_.isInstanceOf[DateKeyPlanner])
      if (i == -1) {
        keyPlanner
      } else {
        val partitioned = planners.splitAt(i + 1)
        PartialRowPlanner(partitioned._1, partitioned._2, sep)
      }
    case _ => keyPlanner
  }

  override def getOtherIteratorConfigs(filter: KeyPlanningFilter,
                                       keyPlanner: KeyPlanner,
                                       schema: String,
                                       output: ExplainerOutputType): List[IteratorSetting] = {
    Some(keyPlanner).collect {
      case PartialRowPlanner(primary, secondary, sep) if (secondary.size == 1) =>
        secondary.head.getKeyPlan(filter, false, ExplainNull)
      case PartialRowPlanner(primary, secondary, sep) if (secondary.size > 1) =>
        CompositePlanner(secondary, sep).getKeyPlan(filter, false, ExplainNull)
    }.collect {
      case KeyList(keys) => keys
      case KeyListTiered(keys, _) => keys
      case KeyRange(start, end) if start == end => Seq(start)
      case KeyRanges(ranges) if ranges.forall(r => r.start == r.end) => ranges.map(_.start)
    }.map { suffixes =>
      val cfg = new IteratorSetting(iteratorPriority_RowSkippingIterator, classOf[RowSkippingIterator])
      RowSkippingIterator.configure(cfg, suffixes)
      output(s"Row Suffixes (${suffixes.size}): ${suffixes.take(20).mkString(",")}")
      List(cfg)
    }.getOrElse(List.empty)
  }
}

object TimeIndexStrategy extends StrategyProvider {

  override def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints = NoopHints) = {
    val dtgOption = core.index.getDtgFieldName(sft)
    dtgOption.flatMap { case dtg =>
      val indexed: (PropertyLiteral) => Boolean = (p: PropertyLiteral) => p.name == dtg
      filter match {
        case f: PropertyIsEqualTo =>
          checkOrder(f.getExpression1, f.getExpression2).filter(indexed).map { property =>
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = hints.temporalCost(value, value)
            StrategyDecision(new TimeIndexStrategy, cost)
          }

        case f: TEquals =>
          checkOrder(f.getExpression1, f.getExpression2).filter(indexed).map { property =>
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = hints.temporalCost(value, value)
            StrategyDecision(new TimeIndexStrategy, cost)
          }

        case f: PropertyIsBetween =>
          val name = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (name == dtg) {
            val lower = f.getLowerBoundary.asInstanceOf[Literal].evaluate(null, classOf[Date])
            val upper = f.getUpperBoundary.asInstanceOf[Literal].evaluate(null, classOf[Date])
            val cost = hints.temporalCost(lower, upper)
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          } else {
            None
          }

        case f: PropertyIsGreaterThan =>
          checkOrder(f.getExpression1, f.getExpression2).filter(indexed).map { property =>
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.temporalCost(new Date(0), value)
            } else {
              hints.temporalCost(value, new Date()) // TODO project into future?
            }
            StrategyDecision(new TimeIndexStrategy, cost)
          }

        case f: PropertyIsGreaterThanOrEqualTo =>
          checkOrder(f.getExpression1, f.getExpression2).filter(indexed).map { property =>
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.temporalCost(new Date(0), value)
            } else {
              hints.temporalCost(value, new Date()) // TODO project into future?
            }
            StrategyDecision(new TimeIndexStrategy, cost)
          }

        case f: PropertyIsLessThan =>
          checkOrder(f.getExpression1, f.getExpression2).filter(indexed).map { property =>
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.temporalCost(value, new Date()) // TODO project into future?
            } else {
              hints.temporalCost(new Date(0), value)
            }
            StrategyDecision(new TimeIndexStrategy, cost)
          }

        case f: PropertyIsLessThanOrEqualTo =>
          checkOrder(f.getExpression1, f.getExpression2).filter(indexed).map { property =>
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.temporalCost(value, new Date()) // TODO project into future?
            } else {
              hints.temporalCost(new Date(0), value)
            }
            StrategyDecision(new TimeIndexStrategy, cost)
          }

        case f: Before =>
          checkOrder(f.getExpression1, f.getExpression2).filter(indexed).map { property =>
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.temporalCost(value, new Date()) // TODO project into future?
            } else {
              hints.temporalCost(new Date(0), value)
            }
            StrategyDecision(new TimeIndexStrategy, cost)
          }

        case f: After =>
          checkOrder(f.getExpression1, f.getExpression2).filter(indexed).map { property =>
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.temporalCost(new Date(0), value)
            } else {
              hints.temporalCost(value, new Date()) // TODO project into future?
            }
            StrategyDecision(new TimeIndexStrategy, cost)
          }

        case f: During =>
          checkOrder(f.getExpression1, f.getExpression2).filter(indexed).map { property =>
            val during = property.literal.getValue.asInstanceOf[DefaultPeriod]
            val lower = during.getBeginning.getPosition.getDate
            val upper = during.getEnding.getPosition.getDate
            val cost = hints.temporalCost(lower, upper)
            StrategyDecision(new TimeIndexStrategy, cost)
          }

        // doesn't match any temporal strategy
        case _ => None
      }
    }
  }
}

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

import java.util.Date

import org.geotools.data.Query
import org.geotools.temporal.`object`.DefaultPeriod
import org.locationtech.geomesa.core
import org.locationtech.geomesa.core.data.AccumuloConnectorCreator
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.temporal.{After, Before, During, TEquals}

class TimeIndexStrategy extends Strategy {

  override def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       query: Query,
                       output: ExplainerOutputType) = {

    null
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

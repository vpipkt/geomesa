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

import org.apache.accumulo.core.data.Range
import org.geotools.data.Query
import org.geotools.temporal.`object`.DefaultPeriod
import org.locationtech.geomesa.core
import org.locationtech.geomesa.core.data.AccumuloConnectorCreator
import org.locationtech.geomesa.core.data.tables.AttributeTable
import org.locationtech.geomesa.core.index.AttributeIndexStrategy._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.temporal.{During, After, Before, TEquals}
import org.opengis.filter._

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

  override def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints) = {
    val dtgOption = core.index.getDtgFieldName(sft)
    dtgOption.flatMap { case dtg =>
      filter match {
        case f: PropertyIsEqualTo =>
          val property = checkOrder(f.getExpression1, f.getExpression2)
          if (property.name != dtg) None else {
            val descriptor = sft.getDescriptor(property.name)
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = hints.attributeCost(descriptor, value, value)
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          }

        case f: TEquals =>
          val property = checkOrder(f.getExpression1, f.getExpression2)
          if (property.name != dtg) None else {
            val descriptor = sft.getDescriptor(property.name)
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = hints.attributeCost(descriptor, value, value)
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          }

        case f: PropertyIsBetween =>
          val name = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          if (name == dtg) {
            val lower = f.getLowerBoundary.asInstanceOf[Literal].evaluate(null, classOf[Date])
            val upper = f.getUpperBoundary.asInstanceOf[Literal].evaluate(null, classOf[Date])
            val descriptor = sft.getDescriptor(name)
            val cost = hints.attributeCost(descriptor, lower, upper)
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          } else {
            None
          }

        case f: PropertyIsGreaterThan =>
          val property = checkOrder(f.getExpression1, f.getExpression2)
          if (property.name != dtg) None else {
            val descriptor = sft.getDescriptor(property.name)
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.attributeCost(descriptor, new Date(0), value)
            } else {
              hints.attributeCost(descriptor, value, new Date()) // TODO project into future?
            }
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          }

        case f: PropertyIsGreaterThanOrEqualTo =>
          val property = checkOrder(f.getExpression1, f.getExpression2)
          if (property.name != dtg) None else {
            val descriptor = sft.getDescriptor(property.name)
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.attributeCost(descriptor, new Date(0), value)
            } else {
              hints.attributeCost(descriptor, value, new Date()) // TODO project into future?
            }
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          }

        case f: PropertyIsLessThan =>
          val property = checkOrder(f.getExpression1, f.getExpression2)
          if (property.name != dtg) None else {
            val descriptor = sft.getDescriptor(property.name)
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.attributeCost(descriptor, value, new Date()) // TODO project into future?
            } else {
              hints.attributeCost(descriptor, new Date(0), value)
            }
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          }

        case f: PropertyIsLessThanOrEqualTo =>
          val property = checkOrder(f.getExpression1, f.getExpression2)
          if (property.name != dtg) None else {
            val descriptor = sft.getDescriptor(property.name)
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.attributeCost(descriptor, value, new Date()) // TODO project into future?
            } else {
              hints.attributeCost(descriptor, new Date(0), value)
            }
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          }

        case f: Before =>
          val property = checkOrder(f.getExpression1, f.getExpression2)
          if (property.name != dtg) None else {
            val descriptor = sft.getDescriptor(property.name)
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.attributeCost(descriptor, value, new Date()) // TODO project into future?
            } else {
              hints.attributeCost(descriptor, new Date(0), value)
            }
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          }

        case f: After =>
          val property = checkOrder(f.getExpression1, f.getExpression2)
          if (property.name != dtg) None else {
            val descriptor = sft.getDescriptor(property.name)
            val value = property.literal.evaluate(null, classOf[Date])
            val cost = if (property.flipped) {
              hints.attributeCost(descriptor, new Date(0), value)
            } else {
              hints.attributeCost(descriptor, value, new Date()) // TODO project into future?
            }
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          }

        case f: During =>
          val property = checkOrder(f.getExpression1, f.getExpression2)
          if (property.name != dtg) None else {
            val descriptor = sft.getDescriptor(property.name)
            val during = property.literal.getValue.asInstanceOf[DefaultPeriod]
            val lower = during.getBeginning.getPosition.getDate
            val upper = during.getEnding.getPosition.getDate
            val cost = hints.attributeCost(descriptor, lower, upper)
            Some(StrategyDecision(new TimeIndexStrategy, cost))
          }

        // doesn't match any temporal strategy
        case _ => None
      }
    }
  }
}

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

import java.util.Map.Entry

import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.Query
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.util.SelfClosingIterator
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.spatial.BinarySpatialOperator

class STIdxStrategy extends BaseSpatioTemporalStrategy {

  def execute(query: Query,
              featureType: SimpleFeatureType,
              indexSchema: String,
              featureEncoding: FeatureEncoding,
              acc: AccumuloConnectorCreator,
              output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val bs = acc.createSTIdxScanner(featureType)
    tryScanner(query, featureType, indexSchema, featureEncoding, bs, output)
  }
}

object STIdxStrategy extends StrategyProvider {

  import org.locationtech.geomesa.core.filter.spatialFilters
  import org.locationtech.geomesa.core.index.strategies.BaseSpatioTemporalStrategy._

  override def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints = NoopHints) =
    if (spatialFilters(filter)) {
      val e1 = filter.asInstanceOf[BinarySpatialOperator].getExpression1
      val e2 = filter.asInstanceOf[BinarySpatialOperator].getExpression2
      checkOrder(e1, e2).filter(p => p.name == sft.getGeometryDescriptor.getLocalName).map { property =>
        val geomToCover = getGeometryToCover(Seq(filter), sft)._2
        val cost = hints.spatialCost(geomToCover)
        StrategyDecision(new STIdxStrategy, cost)
      }
    } else {
      None
    }
}

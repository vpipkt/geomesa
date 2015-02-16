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

package org.locationtech.geomesa.core.data.tables

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{BatchDeleter, BatchWriter}
import org.apache.accumulo.core.data.Key
import org.locationtech.geomesa.core.data.AccumuloFeatureWriter.FeatureWriterFn
import org.locationtech.geomesa.core.index.{IndexEntryEncoder, _}
import org.opengis.feature.simple.SimpleFeatureType

object TimeIndexTable extends Logging {

  // index rows have an index flag as part of the schema
  def isIndexEntry(key: Key): Boolean = SpatioTemporalTable.isIndexEntry(key)

  // data rows have a data flag as part of the schema
  def isDataEntry(key: Key): Boolean = SpatioTemporalTable.isDataEntry(key)

  def timeIndexWriter(bw: BatchWriter, encoder: IndexEntryEncoder): FeatureWriterFn =
    SpatioTemporalTable.spatioTemporalWriter(bw, encoder)

  /** Creates a function to remove spatio temporal index entries for a feature **/
  def removeTimeIndex(bw: BatchWriter, encoder: IndexEntryEncoder): FeatureWriterFn =
    SpatioTemporalTable.removeSpatioTemporalIdx(bw, encoder)

  def deleteFeaturesFromTable(bd: BatchDeleter, sft: SimpleFeatureType): Unit =
    getTimeIndexSchema(sft).foreach { schema =>
      SpatioTemporalTable.deleteFeaturesFromTable(bd, schema)
    }
}

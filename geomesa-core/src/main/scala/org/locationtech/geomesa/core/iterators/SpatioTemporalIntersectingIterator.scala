/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.core.iterators

import java.util.{Collections, HashSet => JHashSet}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{ArrayByteSequence, ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index.IndexEntry.DecodedIndexValue
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConverters._

case class Attribute(name: Text, value: Text)

/**
 * Remember that we maintain two sources:  one for index records (that determines
 * whether the point is inside the search polygon), and one for data records (that
 * can consist of multiple data rows per index row, one data-row per attribute).
 * Each time the index source advances, we need to reposition the data source.
 *
 * This iterator returns as its nextKey and nextValue responses the key and value
 * from the DATA iterator, not from the INDEX iterator.  The assumption is that
 * the data rows are what we care about; that we do not care about the index
 * rows that merely helped us find the data rows quickly.
 *
 * The other trick to remember about iterators is that they essentially pre-fetch
 * data.  "hasNext" really means, "was there a next record that you already found".
 */
class SpatioTemporalIntersectingIterator
  extends SortedKeyValueIterator[Key, Value]
  with Logging with WrappedSTFilter {

  protected var indexSource: SortedKeyValueIterator[Key, Value] = null
  protected var dataSource: SortedKeyValueIterator[Key, Value] = null
  protected var topKey: Key = null
  protected var topValue: Value = null
  protected var nextKey: Key = null
  protected var nextValue: Value = null
//  protected var curId: Text = null

  protected var deduplicate: Boolean = false

  // each batch-scanner thread maintains its own (imperfect!) list of the
  // unique (in-polygon) identifiers it has seen
  protected var maxInMemoryIdCacheEntries = 10000
  protected val inMemoryIdCache = new JHashSet[String]()

  def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    TServerClassLoader.initClassLoader(logger)

    val featureType = SimpleFeatureTypes.createType("DummyType", options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE))
    featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    dateAttributeName = getDtgFieldName(featureType)

    if (options.containsKey(DEFAULT_FILTER_PROPERTY_NAME)) {
      val filterString  = options.get(DEFAULT_FILTER_PROPERTY_NAME)
      filter = ECQL.toFilter(filterString)
      val sfb = new SimpleFeatureBuilder(featureType)
      testSimpleFeature = sfb.buildFeature("test")
    }

    if (options.containsKey(DEFAULT_CACHE_SIZE_NAME))
      maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt

    if (!options.containsKey(GEOMESA_ITERATORS_IS_DENSITY_TYPE)) {
      deduplicate = IndexSchema.mayContainDuplicates(featureType)
    }

    this.indexSource = source.deepCopy(env)
    this.dataSource = source.deepCopy(env)
  }

  //def hasTop = nextKey != null || topKey != null
  def hasTop = topKey != null

  def getTopKey = topKey

  def getTopValue = topValue

  /**
   * Returns a local estimate as to whether the current identifier
   * is likely to be a duplicate.
   *
   * Because we set a limit on how many unique IDs will be preserved in
   * the local cache, a TRUE response is always accurate, but a FALSE
   * response may not be accurate.  (That is, this cache allows for false-
   * negatives, but no false-positives.)  We accept this, because there is
   * a final, client-side filter that will eliminate all duplicate IDs
   * definitively.  The purpose of the local cache is to reduce traffic
   * through the remainder of the iterator/aggregator pipeline as quickly as
   * possible.
   *
   * @return False if this identifier is in the local cache; True otherwise
   */
  lazy val isIdUnique: (String) => Boolean =
    if (deduplicate) (id:String) => (id!=null) && !inMemoryIdCache.contains(id)
    else                       _ => true

  lazy val rememberId: (String) => Unit =
    if (deduplicate) (id: String) => {
      if (id!=null && !inMemoryIdCache.contains(id) && inMemoryIdCache.size < maxInMemoryIdCacheEntries)
        inMemoryIdCache.add(id)
    } else _ => Unit

  // data rows are the only ones with "SimpleFeatureAttribute" in the ColQ
  // (if we expand on the idea of separating out attributes more, we will need
  // to revisit this function)
  protected def isKeyValueADataEntry(key: Key, value: Value): Boolean =
    (key != null) &&
    (key.getColumnQualifier != null) &&
    (key.getColumnQualifier == DATA_CQ)

  // if it's not a data entry, it's an index entry
  // (though we still share some requirements -- non-nulls -- with data entries)
  protected def isKeyValueAnIndexEntry(key: Key, value: Value): Boolean =
    (key != null) &&
    (
      (key.getColumnQualifier == null) ||
      (key.getColumnQualifier != DATA_CQ)
    )

  def skipIndexEntries(itr: SortedKeyValueIterator[Key,Value]) {
    while (itr != null && itr.hasTop && isKeyValueAnIndexEntry(itr.getTopKey, itr.getTopValue))
    itr.next()
  }

  def skipDataEntries(itr: SortedKeyValueIterator[Key,Value]) {
    while (itr != null && itr.hasTop && isKeyValueADataEntry(itr.getTopKey, itr.getTopValue))
      itr.next()
  }

  /**
   * Advances the index-iterator to the next qualifying entry, and then
   * updates the data-iterator to match what ID the index-iterator found
   */
  def findTop() {
    // clear out the reference to the next entry
    nextKey = null
    nextValue = null

    // be sure to start on an index entry
    skipDataEntries(indexSource)

    while (nextValue == null && indexSource.hasTop && indexSource.getTopKey != null) {
      // the value contains the full-resolution geometry and time; use them
      val decodedValue = IndexEntry.decodeIndexValue(indexSource.getTopValue)
      def isSTAcceptable = wrappedSTFilter(decodedValue.geom, decodedValue.dtgMillis)
      // see whether this box is acceptable
      // (the tests are ordered from fastest to slowest to take advantage of
      // short-circuit evaluation)
      if (isIdUnique(decodedValue.id) && isSTAcceptable) {
        // stash this ID
        rememberId(decodedValue.id)
        // advance the data-iterator to its corresponding match
        seekData(decodedValue)
      }

      // you MUST advance to the next key
      indexSource.next()

      // skip over any intervening data entries, should they exist
      skipDataEntries(indexSource)
    }
  }

  /**
   * Updates the data-iterator to seek the first row that matches the current
   * (top) reference of the index-iterator.
   *
   * We emit the top-key from the index-iterator, and the top-value from the
   * data-iterator.  This is *IMPORTANT*, as otherwise we do not emit rows
   * that honor the SortedKeyValueIterator expectation, and Bad Things Happen.
   */
  def seekData(indexValue: IndexEntry.DecodedIndexValue) {
    val nextId = indexValue.id
//    curId = new Text(nextId)
    val indexSourceTopKey = indexSource.getTopKey

//    val dataSeekKey = new Key(indexSourceTopKey.getRow, curId)
    val dataSeekKey = new Key(indexSourceTopKey.getRow, null)
    val range = new Range(dataSeekKey, null)
    val colFamilies = List[ByteSequence](new ArrayByteSequence(nextId.getBytes)).asJavaCollection
    dataSource.seek(range, colFamilies, true)

    // it may be possible to pollute the key space so that index rows can be
    // confused for data rows; skip until you know you've found a data row
    skipIndexEntries(dataSource)

    if (!dataSource.hasTop || dataSource.getTopKey == null || dataSource.getTopKey.getColumnFamily.toString != nextId)
      logger.error(s"Could not find the data key corresponding to index key $indexSourceTopKey and dataId is $nextId.")
    else {
      nextKey = new Key(indexSourceTopKey)
      nextValue = dataSource.getTopValue
    }
  }

  val curRow = new Text()
  val idxCandidates = Array.ofDim[(Key, DecodedIndexValue, Int)](1024)
  var curRowSize = -1
  var nextIdx = -1
  def collectIndexCandidates() = {
    skipDataEntries(indexSource)
    if (indexSource.hasTop) {
      indexSource.getTopKey.getRow(curRow)

      var i = 0
      while (indexSource.hasTop && indexSource.getTopKey.compareRow(curRow) == 0) {
        if (isKeyValueAnIndexEntry(indexSource.getTopKey, indexSource.getTopValue)) {
          val decodedValue = IndexEntry.decodeIndexValue(indexSource.getTopValue)
          def isSTAcceptable = wrappedSTFilter(decodedValue.geom, decodedValue.dtgMillis)
          // see whether this box is acceptable
          // (the tests are ordered from fastest to slowest to take advantage of
          // short-circuit evaluation)
          if (isIdUnique(decodedValue.id) && isSTAcceptable) {
            // stash this ID
            rememberId(decodedValue.id)
            idxCandidates(i) = (new Key(indexSource.getTopKey), decodedValue, i)
            i += 1
          }
        }
        indexSource.next()
      }
      if (i > 0) {
        curRowSize = i
        nextIdx = 0
      }
    }
  }

  val dataCandidates = Array.ofDim[(Key, Value)](1024)
  def collectDataCandidates() = {
    if (curRowSize > 0) {
      val sorted = idxCandidates.take(curRowSize).sortBy(_._2.id)

      val dataSeekKey = new Key(sorted(0)._1.getRow, new Text(sorted(0)._2.id))
      val range = new Range(dataSeekKey, null)
      val colFamilies = Collections.singleton[ByteSequence](new ArrayByteSequence(sorted(0)._2.id.getBytes))
      dataSource.seek(range, colFamilies, true)
      skipIndexEntries(dataSource)

      sorted.foreach { el =>
        val curId = new Text(el._2.id)
        while (dataSource.getTopKey.compareColumnFamily(curId) != 0) {
          dataSource.next()
          //skipIndexEntries(dataSource)
        }
        dataCandidates(el._3) = (el._1, new Value(dataSource.getTopValue))
      }
      //dataSource.next()
    }
  }

  /**
   * If there was a next, then we pre-fetched it, so we report those entries
   * back to the user, and make an attempt to pre-fetch another row, allowing
   * us to know whether there exists, in fact, a next entry.
   */
  def next() {
    if(curRowSize == -1) {
      collectIndexCandidates()
      collectDataCandidates()
      if(curRowSize > 0) {
        topKey = dataCandidates(0)._1
        topValue = dataCandidates(0)._2
        nextIdx = 1
      }
    } else if(nextIdx < curRowSize) {
      topKey = dataCandidates(nextIdx)._1
      topValue = dataCandidates(nextIdx)._2
      nextIdx += 1
    } else {
      topKey = null
      topValue = null
      curRowSize = -1
      nextIdx = -1
      collectIndexCandidates()
      collectDataCandidates()
      if(curRowSize > 0) {
        topKey = dataCandidates(0)._1
        topValue = dataCandidates(0)._2
        nextIdx = 1
      }
    }
  }

  /**
   * Position the index-source.  Consequently, updates the data-source.
   *
   * @param range
   * @param columnFamilies
   * @param inclusive
   */
  def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    // move the source iterator to the right starting spot
    indexSource.seek(range, columnFamilies, inclusive)
    next()
  }

  def deepCopy(env: IteratorEnvironment) = throw new UnsupportedOperationException("STII does not support deepCopy.")
}

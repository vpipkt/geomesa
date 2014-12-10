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

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.core.data.{FeatureEncoding, SimpleFeatureDecoder, SimpleFeatureEncoder}
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.transform.TransformCreator
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait WrappedFeatureType {

  var featureType: SimpleFeatureType = null

  // feature type config
  def initFeatureType(options: java.util.Map[String, String]) = {
    val sftName = Option(options.get(GEOMESA_ITERATORS_SFT_NAME)).getOrElse(this.getClass.getCanonicalName)
    featureType = SimpleFeatureTypes.createType(sftName, options.get(
      GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE))
    featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }
}

trait WrappedFeatureBuilder extends WrappedFeatureType {

  import org.locationtech.geomesa.core.iterators.IteratorTrigger._

  var featureBuilder: SimpleFeatureBuilder = null

  lazy val geomIdx = featureType.indexOf(featureType.getGeometryDescriptor.getLocalName)
  lazy val sdtgIdx = featureType.startTimeName.map { n => featureType.indexOf(n) }.getOrElse(-1)
  lazy val edtgIdx = featureType.endTimeName.map { n => featureType.indexOf(n) }.getOrElse(-1)
  lazy val hasDtg = featureType.startTimeName.isDefined

  private val attrArrayPool = ObjectPoolFactory(Array.ofDim[AnyRef](featureType.getAttributeCount))

  override def initFeatureType(options: java.util.Map[String, String]) = {
    super.initFeatureType(options)
    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(featureType)
  }

  /**
   * Converts values taken from the Index Value to a SimpleFeature, using the passed SimpleFeatureBuilder
   * Note that the ID, taken from the index, is preserved
   * Also note that the SimpleFeature's other attributes may not be fully parsed and may be left as null;
   * the SimpleFeatureFilteringIterator *may* remove the extraneous attributes later in the Iterator stack
   */
  def encodeIndexValueToSF(id: String,
                           geom: Geometry,
                           dtgMillis: Option[Long]): SimpleFeature = {
    val dtgDate = dtgMillis.map(time => new Date(time))

    // Build and fill the Feature. This offers some performance gain over building and then setting the attributes.
    attrArrayPool.withResource { attrArray =>
      fillAttributeArray(geom, dtgDate, attrArray)
      featureBuilder.buildFeature(id, attrArray)
    }
  }

  /**
   * Construct and fill an array of the SimpleFeature's attribute values
   * Reuse attrArray as it is copied inside of feature builder anyway
   */
  private def fillAttributeArray(geomValue: Geometry,
                                 date: Option[java.util.Date],
                                 attrArray: Array[AnyRef]) = {
    // always set the mandatory geo element
    attrArray(geomIdx) = geomValue
    if(hasDtg) {
      // if dtgDT exists, attempt to fill the elements corresponding to the start and/or end times
      date.foreach { time =>
        if(sdtgIdx != -1) attrArray(sdtgIdx) = time
        if(edtgIdx != -1) attrArray(edtgIdx) = time
      }
    }
  }
}

trait WrappedSTFilter {

  var stFilter: Option[Filter] = None
  var dateAttributeName: Option[String] = None
  var testSimpleFeature: Option[SimpleFeature] = None

  lazy val wrappedSTFilter: Option[(Geometry, Option[Long]) => Boolean] =
    for (filter <- stFilter.filterNot(_ == Filter.INCLUDE); feat <- testSimpleFeature) yield {
      (geom: Geometry, olong: Option[Long]) => {
        feat.setDefaultGeometry(geom)
        dateAttributeName.foreach { dateAttribute =>
          val date = new Date(olong.getOrElse(System.currentTimeMillis()))
          feat.setAttribute(dateAttribute, date)
        }
        filter.evaluate(feat)
      }
    }

  // spatio-temporal filter config
  def initSTFilter(featureType: SimpleFeatureType, options: java.util.Map[String, String]) = {
    dateAttributeName = getDtgFieldName(featureType)

    if (options.containsKey(DEFAULT_FILTER_PROPERTY_NAME)) {
      val filterString = options.get(DEFAULT_FILTER_PROPERTY_NAME)
      stFilter = Some(ECQL.toFilter(filterString))
      val sfb = new SimpleFeatureBuilder(featureType)
      testSimpleFeature = Some(sfb.buildFeature("test"))
    }
  }

}

trait WrappedFeatureDecoder {

  var featureDecoder: SimpleFeatureDecoder = null
  var featureEncoder: SimpleFeatureEncoder = null

  // feature encoder/decoder
  def initDecoder(featureType: SimpleFeatureType, options: java.util.Map[String, String]) = {
    // this encoder is for the source sft
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.AVRO.toString)
    featureDecoder = SimpleFeatureDecoder(featureType, encodingOpt)
    featureEncoder = SimpleFeatureEncoder(featureType, encodingOpt)
  }
}

trait WrappedEcqlFilter {

  var ecqlFilter: Option[Filter] = None

  lazy val wrappedEcqlFilter: Option[(SimpleFeature) => Boolean] =
    ecqlFilter.filterNot(_ == Filter.INCLUDE).map { filter =>
      (sf: SimpleFeature) => filter.evaluate(sf)
    }

  // other filter config
  def initEcqlFilter(options: java.util.Map[String, String]) =
    if (options.containsKey(GEOMESA_ITERATORS_ECQL_FILTER)) {
      val filterString = options.get(GEOMESA_ITERATORS_ECQL_FILTER)
      ecqlFilter = Some(ECQL.toFilter(filterString))
    }
}

trait WrappedTransform {

  var targetFeatureType: Option[SimpleFeatureType] = None
  var transformString: Option[String] = None
  var transformEncoding: FeatureEncoding = null

  lazy val wrappedTransform: Option[(SimpleFeature) => Array[Byte]] =
    for {
      featureType <- targetFeatureType
      string <- transformString
    } yield {
      val transform = TransformCreator.createTransform(featureType, transformEncoding, string)
      (sf: SimpleFeature) => transform(sf)
    }

  // feature type transforms
  def initTransform(featureType: SimpleFeatureType, options: java.util.Map[String, String]) =
    if (options.containsKey(GEOMESA_ITERATORS_TRANSFORM_SCHEMA)) {
      val transformSchema = options.get(GEOMESA_ITERATORS_TRANSFORM_SCHEMA)

      targetFeatureType = Some(SimpleFeatureTypes.createType(this.getClass.getCanonicalName, transformSchema))
      targetFeatureType.foreach(_.decodeUserData(options, GEOMESA_ITERATORS_TRANSFORM_SCHEMA))

      transformString = Option(options.get(GEOMESA_ITERATORS_TRANSFORM))
      transformEncoding = Option(options.get(FEATURE_ENCODING)).map(FeatureEncoding.withName(_))
          .getOrElse(FeatureEncoding.AVRO)
    }
}

trait InMemoryDeduplication {

  var deduplicate: Boolean = false

  // each thread maintains its own (imperfect!) list of the unique identifiers it has seen
  var maxInMemoryIdCacheEntries = 10000
  var inMemoryIdCache: java.util.HashSet[String] = null

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
  lazy val checkUniqueId: Option[(String) => Boolean] =
    Some(deduplicate).filter(_ == true).map { _ =>
      (id: String) =>
        Option(id)
          .filter(_ => inMemoryIdCache.size < maxInMemoryIdCacheEntries)
          .forall(inMemoryIdCache.add(_))
    }

  def initDeduplication(featureType: SimpleFeatureType, options: java.util.Map[String, String]) = {
    // check for dedupe - we don't need to dedupe for density queries
    if (!options.containsKey(GEOMESA_ITERATORS_IS_DENSITY_TYPE)) {
      deduplicate = IndexSchema.mayContainDuplicates(featureType)
      if (deduplicate) {
        if (options.containsKey(DEFAULT_CACHE_SIZE_NAME)) {
          maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
        }
        inMemoryIdCache = new java.util.HashSet[String](maxInMemoryIdCacheEntries)
      }
    }
  }
}
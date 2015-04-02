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

package org.locationtech.geomesa.compute.spark

import java.text.SimpleDateFormat
import java.util.UUID

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase, InputConfigurator}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStore, DataStoreFinder, DefaultTransaction, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce._
import org.locationtech.geomesa.core.index.{ExplainPrintln, STIdxStrategy, _}
import org.locationtech.geomesa.feature._
import org.locationtech.geomesa.feature.kryo.{KryoFeatureSerializer, SimpleFeatureSerializer}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

object GeoMesaSpark {

  def init(conf: SparkConf, ds: DataStore): SparkConf = {
    val typeOptions = ds.getTypeNames.map { t => (t, SimpleFeatureTypes.encodeType(ds.getSchema(t))) }
    typeOptions.foreach { case (k,v) => System.setProperty(typeProp(k), v) }
    val extraOpts = typeOptions.map { case (k,v) => jOpt(k, v) }.mkString(" ")

    conf.set("spark.executor.extraJavaOptions", extraOpts)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getCanonicalName)
  }

  def typeProp(typeName: String) = s"geomesa.types.$typeName"
  def jOpt(typeName: String, spec: String) = s"-D${typeProp(typeName)}=$spec"

  def rdd(conf: Configuration, params: Map[String, String], sc: SparkContext, query: Query, useMock: Boolean = false): RDD[SimpleFeature] = {
    val filter = ECQL.toCQL(query.getFilter)
    val job = Job.getInstance(conf, "GeoMesa Spark")
    GeoMesaInputFormat.configure(job, params, query.getTypeName, Some(filter), useMock)
    sc.newAPIHadoopRDD(job.getConfiguration, classOf[GeoMesaInputFormat], classOf[Text], classOf[SimpleFeature]).map { case (t, sf) => sf }
  }

  /**
   * Writes this RDD to a GeoMesa table.
   * The type must exist in the data store, and all of the features in the RDD must be of this type.
   * @param rdd
   * @param writeDataStoreParams
   * @param writeTypeName
   */
  def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    val ds = DataStoreFinder.getDataStore(writeDataStoreParams).asInstanceOf[AccumuloDataStore]
    require(ds.getSchema(writeTypeName) != null, "feature type must exist before calling save.  Call .createSchema on the DataStore before calling .save")

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(writeDataStoreParams).asInstanceOf[AccumuloDataStore]
      val transaction = new DefaultTransaction(UUID.randomUUID().toString)
      val featureWriter = ds.getFeatureWriterAppend(writeTypeName, transaction)
      val attrNames = featureWriter.getFeatureType.getAttributeDescriptors.map(_.getLocalName)
      try {
        iter.foreach { case rawFeature =>
          val newFeature = featureWriter.next()
          attrNames.foreach(an => newFeature.setAttribute(an, rawFeature.getAttribute(an)))
          featureWriter.write()
        }
        transaction.commit()
      } finally {
        featureWriter.close()
      }
    }
  }

  def countByDay(conf: Configuration, sccc: SparkContext, params: Map[String, String], query: Query, dateField: String = "dtg") = {
    val d = rdd(conf, params, sccc, query)
    val dayAndFeature = d.mapPartitions { iter =>
      val df = new SimpleDateFormat("yyyyMMdd")
      val ff = CommonFactoryFinder.getFilterFactory2
      val exp = ff.property(dateField)
      iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
    }
    val groupedByDay = dayAndFeature.groupBy { case (date, _) => date }
    groupedByDay.map { case (date, iter) => (date, iter.size) }
  }

}

class GeoMesaSparkKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    val serializer = new com.esotericsoftware.kryo.Serializer[SimpleFeature]() {
      val typeCache = CacheBuilder.newBuilder().build(
        new CacheLoader[String, SimpleFeatureType] {
          override def load(key: String): SimpleFeatureType = {
            val spec = System.getProperty(GeoMesaSpark.typeProp(key))
            if (spec == null) throw new IllegalArgumentException(s"Couldn't find property geomesa.types.$key")
            SimpleFeatureTypes.createType(key, spec)
          }
        })

      val serializerCache = CacheBuilder.newBuilder().build(
        new CacheLoader[String, SimpleFeatureSerializer] {
          override def load(key: String): SimpleFeatureSerializer = new SimpleFeatureSerializer(typeCache.get(key))
        })


      override def write(kryo: Kryo, out: Output, feature: SimpleFeature): Unit = {
        val typeName = feature.getFeatureType.getTypeName
        out.writeString(typeName)
        serializerCache.get(typeName).write(kryo, out, feature)
      }

      override def read(kry: Kryo, in: Input, clazz: Class[SimpleFeature]): SimpleFeature = {
        val typeName = in.readString()
        serializerCache.get(typeName).read(kryo, in, clazz)
      }
    }

    KryoFeatureSerializer.setupKryo(kryo, serializer)
  }
}

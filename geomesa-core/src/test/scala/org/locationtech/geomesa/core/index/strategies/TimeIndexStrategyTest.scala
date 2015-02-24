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
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.TestWithDataStore
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.feature.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TimeIndexStrategyTest extends Specification with TestWithDataStore {

  // required to avoid kryo encoding conflicts
  sequential

  override val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  override def getTestFeatures() = {
    val sameDay = (0 until 5).map { i =>
      val name = s"$i"
      val date = s"2014-01-01T1$i:00:00.000Z"
      val geom = WKTUtils.read(s"POINT(45.0 5$i.0)")
      ScalaSimpleFeatureFactory.buildFeature(sft, Seq(name, date, geom), s"fid-$i")
    }
    val diffDays = (5 until 10).map { i =>
      val name = s"$i"
      val date = s"2014-01-0${i}T12:00:00.000Z"
      val geom = WKTUtils.read(s"POINT(45.0 5$i.0)")
      ScalaSimpleFeatureFactory.buildFeature(sft, Seq(name, date, geom), s"fid-$i")
    }
    sameDay ++ diffDays
  }

  populateFeatures

  // hints that will always select time strategy
  val hints = new StrategyHints {
    override def temporalCost(start: Date, end: Date) = 1
    override def attributeCost[T](ad: AttributeDescriptor, start: T, end: T) = 999
    override def idCost(ids: Seq[String]) = 999
    override def spatialCost(geomToCover: Geometry) = 999
  }

  val featureEncoding = ds.getFeatureEncoding(sft)
  val queryPlanner = new QueryPlanner(sft, featureEncoding, null, ds.getTimeIndexSchemaFmt(sftName), ds, hints)

  def execute(filter: String): List[SimpleFeature] = {
    val query = new Query(sftName, ECQL.toFilter(filter))
    AccumuloDataStore.setQueryTransforms(query, sft)
    // queryPlanner.planQuery(query, ExplainPrintln)
    // println("\n\n")
    queryPlanner.query(query).toList
  }

  "TimeIndexStrategy" should {
    "print values" in {
      skipped("used for debugging")
      val scanner = connector.createScanner(ds.getTimeIndexTableName(sftName), new Authorizations())
      scanner.foreach(println)
      println
      success
    }
    "query on greater than" in {
      val features = execute("dtg > '2014-01-02T00:00:00.000Z'")
      features must have size(5)
      features.map(_.getID) must contain(exactly("fid-5", "fid-6", "fid-7", "fid-8", "fid-9"))
    }
    "query on less than" in {
      val features = execute("dtg < '2014-01-02T00:00:00.000Z'")
      features must have size(5)
      features.map(_.getID) must contain(exactly("fid-0", "fid-1", "fid-2", "fid-3", "fid-4"))
    }
    "query on less than or equals" in {
      val features = execute("dtg <= '2014-01-01T12:00:00.000Z'")
      features must have size(3)
      features.map(_.getID) must contain(exactly("fid-0", "fid-1", "fid-2"))
    }
    "query on greater than or equals" in {
      val features = execute("dtg >= '2014-01-01T12:00:00.000Z'")
      features must have size(8)
      features.map(_.getID) must contain(exactly("fid-2", "fid-3", "fid-4", "fid-5", "fid-6", "fid-7", "fid-8", "fid-9"))
    }
    "query on between" in {
      val features = execute("dtg between '2014-01-01T12:00:00.000Z' AND '2014-01-01T14:00:00.000Z'")
      features must have size(3)
      features.map(_.getID) must contain(exactly("fid-2", "fid-3", "fid-4"))
    }
    "query on equals" in {
      val features = execute("dtg = '2014-01-01T12:00:00.000Z'")
      features must have size(1)
      features.map(_.getID) must contain(exactly("fid-2"))
    }
    "query on before" in {
      val features = execute("dtg BEFORE 2014-01-01T14:00:00.000Z")
      features must have size(4)
      features.map(_.getID) must contain(exactly("fid-0", "fid-1", "fid-2", "fid-3"))
    }
    "query on after" in {
      val features = execute("dtg AFTER 2014-01-01T14:00:00.000Z")
      features must have size(5)
      features.map(_.getID) must contain(exactly("fid-5", "fid-6", "fid-7", "fid-8", "fid-9"))
    }
    "query on during (exclusive)" in {
      val features = execute("dtg DURING 2014-01-01T10:00:00.000Z/2014-01-01T14:00:00.000Z")
      features must have size(3)
      features.map(_.getID) must contain(exactly("fid-1", "fid-2", "fid-3"))
    }
    "query on mixed date and geom" in {
      val features = execute("dtg < '2014-01-02T00:00:00.000Z' AND bbox(geom, 44, 51.5, 46, 53.5)")
      features must have size(2)
      features.map(_.getID) must contain(exactly("fid-2", "fid-3"))
    }
    "query on tequals" in {
      val features = execute("dtg TEQUALS 2014-01-01T12:00:00.000Z")
      features must have size(1)
      features.map(_.getID) must contain(exactly("fid-2"))
    }.pendingUntilFixed("tequals doesn't work with ECQL.toFilter")
  }
}
package org.locationtech.geomesa.convert.text

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CompositeTextConverterTest extends Specification {

  val data =
    """
      |1,hello,45.0,45.0
      |2,world,90.0,90.0
    """.stripMargin

  val conf = ConfigFactory.parseString(
    """
      | converter = {
      |   type         = "composite-converter",
      |   type-name    = "testsft",
      |   converters = [
      |     { converter = "first",   predicate = "strEq('1', substr($0, 0, 1))" },
      |     { converter = "second",  predicate = "strEq('2',  substr($0, 0, 1))" }
      |   ]
      |   first = {
      |     converter = {
      |       type         = "delimited-text",
      |       type-name    = "testsft"
      |       format       = "DEFAULT",
      |       id-field     = "concat('first', $1)",
      |       fields = [
      |         { name = "phrase", transform = "concat($1, $2)" },
      |         { name = "lat",    transform = "$3::double" },
      |         { name = "lon",    transform = "$4::double" },
      |         { name = "geom",   transform = "point($lat, $lon)" }
      |       ]
      |     }
      |   }
      |
      |   second = {
      |     converter = {
      |       type         = "delimited-text",
      |       type-name    = "testsft"
      |       format       = "DEFAULT",
      |       id-field     = "concat('second', $1)",
      |       fields = [
      |         { name = "phrase", transform = "concat($1, $2)" },
      |         { name = "lat",    transform = "$3::double" },
      |         { name = "lon",    transform = "$4::double" },
      |         { name = "geom",   transform = "point($lat, $lon)" }
      |       ]
      |     }
      |   }
      | }
    """.stripMargin)

  "be built from a conf" >> {
    val sft = SimpleFeatureTypes.createType(ConfigFactory.load("sft_testsft.conf"))
    val converter = SimpleFeatureConverters.build[String](sft, conf)
    converter must not beNull

    "and process some data" >> {
      val res = converter.processInput(data.split("\n").toIterator.filterNot( s => "^\\s*$".r.findFirstIn(s).size > 0)).toList
      res.size must be equalTo 2
      res(0).getID must be equalTo "first1"
      res(1).getID must be equalTo "second2"
    }
  }

}

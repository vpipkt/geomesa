package org.locationtech.geomesa.convert.common

import java.util.Date

import com.google.common.hash.Hashing
import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.apache.commons.codec.binary.Base64
import org.joda.time.{DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.Transformers
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TransformersTest extends Specification {

  "Transformers" should {

    implicit val ctx = new EvaluationContext(Map(), null)
    "handle string transformations" >> {

      "trim" >> {
        val exp = Transformers.parse("trim($1)")
        exp.eval("", "foo ", "bar") must be equalTo "foo"
      }

      "capitalize" >> {
        val exp = Transformers.parse("capitalize($1)")
        exp.eval("", "foo", "bar") must be equalTo "Foo"
      }

      "lowercase" >> {
        val exp = Transformers.parse("lowercase($1)")
        exp.eval("", "FOO", "bar") must be equalTo "foo"
      }

      "regexReplace" >> {
        val exp = Transformers.parse("regexReplace('foo'::r,'bar',$1)")
        exp.eval("", "foobar") must be equalTo "barbar"
      }

      "compound expression" >> {
        val exp = Transformers.parse("regexReplace('foo'::r,'bar',trim($1))")
        exp.eval("", " foobar ") must be equalTo "barbar"
      }
    }

    "handle numeric literals" >> {
      val exp = Transformers.parse("$2")
      exp.eval("", "1", 2) must be equalTo 2
    }

    "handle dates" >> {
      val testDate = DateTime.parse("2015-01-01T00:00:00.000Z").toDate

      "date with custom format" >> {
        val exp = Transformers.parse("date('yyyyMMdd', $1)")
        exp.eval("", "20150101").asInstanceOf[Date] must be equalTo testDate
      }

      "isodate" >> {
        val exp = Transformers.parse("isodate($1)")
        exp.eval("", "20150101").asInstanceOf[Date] must be equalTo testDate
      }

      "isodatetime" >> {
        val exp = Transformers.parse("isodatetime($1)")
        exp.eval("", "20150101T000000.000Z").asInstanceOf[Date] must be equalTo testDate
      }

      "dateHourMinuteSecondMillis" >> {
        val exp = Transformers.parse("dateHourMinuteSecondMillis($1)")
        exp.eval("", "2015-01-01T00:00:00.000").asInstanceOf[Date] must be equalTo testDate
      }

    }

    "handle point geometries" >> {
      val exp = Transformers.parse("point($1, $2)")
      exp.eval("", 45.0, 45.0).asInstanceOf[Point].getCoordinate must be equalTo new Coordinate(45.0, 45.0)
    }

    "handle identity functions" >> {
      val bytes = Array.ofDim[Byte](32)
      Random.nextBytes(bytes)
      "md5" >> {
        val hasher = Hashing.md5().newHasher()
        val exp = Transformers.parse("md5($0)")
        val hashedResult = exp.eval(bytes).asInstanceOf[String]
        hashedResult must be equalTo hasher.putBytes(bytes).hash().toString
      }

      "uuid" >> {
        val exp = Transformers.parse("uuid()")
        exp.eval(null) must anInstanceOf[String]
      }

      "base64" >> {
        val exp = Transformers.parse("base64($0)")
        exp.eval(bytes) must be equalTo Base64.encodeBase64URLSafeString(bytes)
      }
    }

    "handle named values" >> {
      val closure = Array.ofDim[Any](3)
      closure(1) = "bar"
      implicit val ctx = new EvaluationContext(Map("foo" -> 1), closure)
      val exp = Transformers.parse("capitalize($foo)")
      exp.eval(null)(ctx) must be equalTo "Bar"
    }
  }

}

package org.locationtech.geomesa.convert.common

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TransformersTest extends Specification {

  val transformers = new Transformers {}
  
  "Transformers" should {

    "handle string transformations" >> {
      "trim" >> {
        val exp = transformers.parse("trim($1)")
        exp.eval("foo ", "bar") must be equalTo "foo"
      }

      "regexReplace" >> {
        val exp = transformers.parse("regexReplace('foo','bar',$0)")
        exp.eval("foobar") must be equalTo "barbar"
      }

      "compound expression" >> {
        val exp = transformers.parse("regexReplace('foo','bar',trim($0))")
        exp.eval(" foobar ") must be equalTo "barbar"
      }
    }

    "handle numeric literals" >> {
      val exp = transformers.parse("$2")
      exp.eval("1", 2) must be equalTo 2
    }
  }

}

package org.locationtech.geomesa.convert.avro

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroTransformersTest extends Specification with AvroUtils {

  sequential

  val transformers = new AvroTransformers {}
  
  "Transformers" should {

    "handle Avro records" >> {

      "extract an inner value" >> {
        val exp = transformers.parse("avroPath($0, '/content$type=TObj/kvmap[$k=prop3]/v')")
        exp.eval(decoded) must be equalTo " foo "
      }

      "handle compound expressions" >> {
        val exp = transformers.parse("trim(avroPath($0, '/content$type=TObj/kvmap[$k=prop3]/v'))")
        exp.eval(decoded) must be equalTo "foo"
      }
    }

  }
}

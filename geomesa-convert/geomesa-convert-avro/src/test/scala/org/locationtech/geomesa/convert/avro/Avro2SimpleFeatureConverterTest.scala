package org.locationtech.geomesa.convert.avro

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Avro2SimpleFeatureConverterTest extends Specification with AvroUtils {

  "Avro2SimpleFeature should" should {

    val conf = ConfigFactory.parseString(
      """
        | converter = {
        |   schema = "/schema.avsc"
        |   sft    = "testsft"
        |   id-field = "uuid()"
        |   fields = [
        |     { name = "lat",  transform = "avroPath($1, '/content$type=TObj/kvmap[$k=lat]/v')" },
        |     { name = "lon",  transform = "avroPath($1, '/content$type=TObj/kvmap[$k=lon]/v')" },
        |     { name = "geom", transform = "point($lon, $lat)" }
        |   ]
        | }
      """.stripMargin)

    "properly convert a GenericRecord to a SimpleFeature" >> {
      val converter = Avro2SimpleFeatureConverterBuilder(conf.getConfig("converter"))
      val sf = converter.convert(converter.fromInputType(bytes), null, null)
      sf.getAttributeCount must be equalTo 1
    }
  }
}

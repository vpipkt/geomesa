package org.locationtech.geomesa.convert.avro

import java.io.ByteArrayOutputStream

import com.typesafe.config.ConfigFactory
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericRecordBuilder, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class Avro2SimpleFeatureConverterTest extends Specification {

  "Avro2SimpleFeature should" should {
    val spec = getClass.getResourceAsStream("/schema.avsc")

    "spec must not be null" >> {
      spec must not beNull
    }

    val parser = new Parser
    val schema = parser.parse(spec)

    val contentSchema = schema.getField("content").schema()
    val types = contentSchema.getTypes.toList
    val tObjSchema = types(0)
    val otherObjSchema = types(1)

    val innerBuilder = new GenericRecordBuilder(tObjSchema.getField("kvmap").schema.getElementType)
    val rec1 = innerBuilder.set("k", "lat").set("v", 45.0).build
    val rec2 = innerBuilder.set("k", "lon").set("v", 45.0).build
    val rec3 = innerBuilder.set("k", "prop3").set("v", null).build
    val rec4 = innerBuilder.set("k", "prop4").set("v", 1.0).build

    val outerBuilder = new GenericRecordBuilder(tObjSchema)
    val tObj = outerBuilder.set("kvmap", List(rec1, rec2, rec3, rec4).asJava).build()

    val compositeBuilder = new GenericRecordBuilder(schema)
    val obj = compositeBuilder.set("content", tObj).build()

    val otherObjBuilder = new GenericRecordBuilder(otherObjSchema)
    val otherObj = otherObjBuilder.set("id", 42).build()
    val obj2 = compositeBuilder.set("content", otherObj).build()

    val baos = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](schema)
    var enc = EncoderFactory.get().binaryEncoder(baos, null)
    writer.write(obj, enc)
    enc.flush()
    baos.close()
    val bytes = baos.toByteArray

    val conf = ConfigFactory.parseString(
    """
      | converter = {
      |   schema = "/schema.avsc"
      |   sft    = "testsft"
      |   fields = [
      |     { name = "lat", path = "/content$type=TObj/kvmap[$k=lat]" },
      |     { name = "lon", path = "/content$type=TObj/kvmap[$k=lon]" }
      |   ]
      | }
    """.stripMargin)

    "properly convert a GenericRecord to a SimpleFeature" >> {
      val converter = Avro2SimpleFeatureConverter(conf.getConfig("converter"))
      val sft = SimpleFeatureTypes.createType("test", "lat:Double,lon:Double")
      val builder = new SimpleFeatureBuilder(sft)
      val reuse = Array.ofDim[AnyRef](sft.getAttributeCount)
      val toPopulate = builder.buildFeature("1")
      val sf = converter.convert(converter.fromByteArray(bytes), toPopulate, reuse)
      sf.getAttributeCount must be equalTo 2
    }
  }
}

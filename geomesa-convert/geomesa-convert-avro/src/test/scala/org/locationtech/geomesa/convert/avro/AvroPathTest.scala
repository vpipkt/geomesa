package org.locationtech.geomesa.convert.avro

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema.Parser
import org.apache.avro.generic._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.util.Utf8
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AvroPathTest extends Specification {

  val spec = """
               |{
               |  "namespace": "org.locationtech",
               |  "type": "record",
               |  "name": "CompositeMessage",
               |  "fields": [
               |    { "name": "content",
               |      "type": [
               |         {
               |           "name": "TObj",
               |           "type": "record",
               |           "fields": [
               |             {
               |               "name": "kvmap",
               |               "type": {
               |                  "type": "array",
               |                  "items": {
               |                    "name": "kvpair",
               |                    "type": "record",
               |                    "fields": [
               |                      { "name": "k", "type": "string" },
               |                      { "name": "v", "type": ["string", "double", "int", "null"] }
               |                    ]
               |                  }
               |               }
               |             }
               |           ]
               |         },
               |         {
               |            "name": "OtherObject",
               |            "type": "record",
               |            "fields": [{ "name": "id", "type": "int"}]
               |         }
               |      ]
               |   }
               |  ]
               |}""".stripMargin

  val parser = new Parser
  val schema = parser.parse(spec)

  val contentSchema = schema.getField("content").schema()
  val types = contentSchema.getTypes.toList
  val tObjSchema = types(0)
  val otherObjSchema = types(1)

  val innerBuilder = new GenericRecordBuilder(tObjSchema.getField("kvmap").schema.getElementType)
  val rec1 = innerBuilder.set("k", "prop1").set("v", "v1").build
  val rec2 = innerBuilder.set("k", "prop2").set("v", "v2").build
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

  val datumReader = new GenericDatumReader[GenericRecord](schema)

  val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
  val gr1 = datumReader.read(null, decoder)

  val baos2 = new ByteArrayOutputStream()
  var enc2 = EncoderFactory.get().binaryEncoder(baos2, null)
  writer.write(obj2, enc2)
  enc2.flush()
  baos2.close()
  val bytes2 = baos2.toByteArray

  val decoder2 = DecoderFactory.get().binaryDecoder(bytes2, null)
  val gr2 = datumReader.read(null, decoder2)

  "AvroPath" should {
    "select a top level path" in {
      val path = "/content"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      val gr = result.get.asInstanceOf[GenericRecord]
      gr.getSchema.getName mustEqual "TObj"
    }

    "select from a union by schema type" in {
      val path = "/content$type=TObj"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      val gr = result.get.asInstanceOf[GenericRecord]
      gr.getSchema.getName mustEqual "TObj"
    }

    "return None when element in union has wrong type" in {
      val path = "/content$type=TObj"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr2)
      result.isDefined mustEqual false
    }

    "return nested records" in {
      val path = "/content$type=TObj/kvmap"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      result.isDefined mustEqual true
      val arr = result.get.asInstanceOf[GenericArray[GenericRecord]]
      arr.length mustEqual 4
    }

    "filter arrays of records by a field predicate" in {
      val path = "/content$type=TObj/kvmap[$k=prop1]"
      val avroPath = AvroPath(path)
      val result = avroPath.eval(gr1)
      result.isDefined mustEqual true
      val r = result.get.asInstanceOf[GenericRecord]
      r.get("k").asInstanceOf[Utf8].toString mustEqual "prop1"
    }

    "select a property out of a record in an array" in {
      "filter arrays of records by a field predicate" in {
        val path = "/content$type=TObj/kvmap[$k=prop1]/v"
        val avroPath = AvroPath(path)
        val result = avroPath.eval(gr1)
        result.isDefined mustEqual true
        val v = result.get.asInstanceOf[String]
        v mustEqual "v1"
      }
    }
  }
}

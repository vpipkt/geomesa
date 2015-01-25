package org.locationtech.geomesa.convert.avro

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.locationtech.geomesa.convert.avro.AvroPath.Expr
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

trait Field {
  def name: String
}

trait ToSimpleFeatureConverter[T] {
  def targetSFT: SimpleFeatureType
  def inputFields: Seq[Field]
  def fromByteArray(bytes: Array[Byte]): T
  def convert(t: T, sf: SimpleFeature, reuse: Array[AnyRef]): SimpleFeature
  
  val indexes = 
    targetSFT.getAttributeDescriptors.flatMap { attr =>
      val targetIdx = targetSFT.indexOf(attr.getName)
      val attrName  = attr.getLocalName
      val inputIdx  = inputFields.indexWhere { f => f.name.equals(attrName) }
      
      if(inputIdx == -1) None
      else               Some((targetIdx, inputIdx))
      
    }.sortBy { case (_, inputIdx) => inputIdx }
  
}

case class AvroField(name: String, path: Expr) extends Field

object Avro2SimpleFeatureConverter {
  def apply(conf: Config): Avro2SimpleFeatureConverter = {
    val avroSchemaPath = conf.getString("schema")
    val avroSchema = new Parser().parse(getClass.getResourceAsStream(avroSchemaPath))
    val reader = new GenericDatumReader[GenericRecord](avroSchema)
    val fields = buildFields(conf.getConfigList("fields"))
    val targetSFT = findTargetSFT(conf.getString("sft"))
    new Avro2SimpleFeatureConverter(avroSchema, reader, targetSFT, fields)
  }

  def findTargetSFT(name: String): SimpleFeatureType = {
    val fileName = s"sft_${name}.conf"
    val conf = ConfigFactory.load(fileName)
    buildSpec(conf)
  }

  def buildSpec(c: Config) = {
    val typeName         = c.getString("type-name")
    val idField          = c.getString("id-field")
    val defaultGeomField = c.getString("default-geom-field")
    val fields           = c.getConfigList("attributes").filterNot(_.getString("name") == idField)

    val fieldSpecs = fields.map { case field =>
      val name    = field.getString("name")
      val default = if (name == defaultGeomField) "*" else ""
      val typ     = field.getString("type")
      val srid    = if (field.hasPath("srid")) s":srid=${field.getString("srid")}" else ""
      val index   = if (field.hasPath("index")) s":index=${field.getBoolean("index")}" else ""
      val stIdx   = if (field.hasPath("index-value")) s":index-value=${field.getBoolean("index-value")}" else ""
      s"$default$name:$typ$srid$index$stIdx"
    }

    val spec = fieldSpecs.mkString(",")
    val sft = SimpleFeatureTypes.createType(typeName, spec)
    sft
  }


  def buildFields(fields: Seq[Config]): IndexedSeq[AvroField] =
    fields.map { f => AvroField(f.getString("name"), AvroPath(f.getString("path"))) }.toIndexedSeq
}

class Avro2SimpleFeatureConverter(avroSchema: Schema,
                                  reader: GenericDatumReader[GenericRecord],
                                  val targetSFT: SimpleFeatureType,
                                  val inputFields: IndexedSeq[AvroField])
  extends ToSimpleFeatureConverter[GenericRecord] {

  var decoder: BinaryDecoder = null
  var reuse: GenericRecord = null

  override def fromByteArray(bytes: Array[Byte]): GenericRecord = {
    decoder = DecoderFactory.get.binaryDecoder(bytes, decoder)
    reader.read(reuse, decoder)
  }

  override def convert(t: GenericRecord, sf: SimpleFeature, reuse: Array[AnyRef]): SimpleFeature = {
    val attributes = 
      if(reuse == null) Array.ofDim[AnyRef](targetSFT.getAttributeCount)
      else reuse
    indexes.foreach { case (targetIdx, inputIdx) =>
      attributes(targetIdx) = inputFields(inputIdx).path.eval(t).orNull
    }
    sf.setAttributes(attributes)
    sf
  }
}

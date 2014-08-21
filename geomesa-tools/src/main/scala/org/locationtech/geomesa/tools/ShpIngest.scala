package org.locationtech.geomesa.tools

import java.io.File

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.SimpleFeature
import org.opengis.referencing.operation.MathTransform

import scala.collection.JavaConversions._

object ShpIngest extends Logging {
  
  def doIngest(config: IngestArguments, dsConf: Map[String, _]): Boolean = {
    val fileUrl = new File(config.file).toURI.toURL
    val params = Map(ShapefileDataStoreFactory.URLP.getName -> fileUrl)
    val shpDataStore = DataStoreFinder.getDataStore(params)
    val featureTypeName = shpDataStore.getTypeNames.head
    val feature = shpDataStore.getFeatureSource(featureTypeName)
    val sourceCRS = feature.getSchema.getCoordinateReferenceSystem
    val targetCRS = DefaultGeographicCRS.WGS84
    val transform = CRS.findMathTransform(sourceCRS, targetCRS, true)

    val ds = DataStoreFinder.getDataStore(dsConf)

    val targetTypeName =
      if(config.featureName.isDefined && !config.featureName.get.equals(featureTypeName)) config.featureName.get
      else featureTypeName

    if(ds.getSchema(targetTypeName) != null) {
      logger.error("Type name already exists")
      false
    }
    else {
      // create the new feature type
      val sftb = new SimpleFeatureTypeBuilder()
      sftb.init(feature.getSchema)
      sftb.setName(targetTypeName)
      val targetType = sftb.buildFeatureType()

      ds.createSchema(targetType)
      val writer = ds.getFeatureWriterAppend(targetTypeName, Transaction.AUTO_COMMIT)
      feature.getFeatures.features().foreach { f =>
        val toWrite = writer.next()
        copyFeature(f, toWrite, transform)
        writer.write()
      }
      writer.close()
      true
    }
  }

  def copyFeature(from: SimpleFeature, to: SimpleFeature, transform: MathTransform): Unit = {
    from.getAttributes.zipWithIndex.foreach { case (attr, idx) => to.setAttribute(idx, attr) }
    to.setDefaultGeometry(JTS.transform(from.getDefaultGeometry.asInstanceOf[Geometry], transform))
    to.getIdentifier.asInstanceOf[FeatureIdImpl].setID(from.getID)
  }
  
}

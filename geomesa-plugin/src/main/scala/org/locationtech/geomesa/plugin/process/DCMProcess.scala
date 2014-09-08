package org.locationtech.geomesa.plugin.process

import java.util

import org.apache.commons.math3.stat.ranking.{NaturalRanking, TiesStrategy}
import org.geoserver.catalog.Catalog
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.DataUtilities
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.factory.{CommonFactoryFinder, GeoTools}
import org.geotools.feature.NameImpl
import org.geotools.geometry.DirectPosition2D
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.plugin.wps.GeomesaProcess
import org.locationtech.geomesa.utils.geotools.GridSnap
import weka.classifiers.functions.Logistic
import weka.core.{Attribute, FastVector, Instance, Instances}

import scala.collection.JavaConversions._
import scala.util.Random


@DescribeProcess(
  title = "Discrete Choice Model",
  description = "Prediction"
)
class DCMProcess(val catalog: Catalog) extends GeomesaProcess {

  @DescribeResult(
    name = "prediction",
    `type` = classOf[GridCoverage2D],
    description = "Prediction")
  def execute(
               @DescribeParameter(
                 name = "predictiveFeatures",
                 collectionType = classOf[String],
                 description = "Predictive Features"
               )
               featureNames: util.Collection[String],

               @DescribeParameter(
                 name = "events",
                 description = "Predict against"
               )
               response: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "width",
                 description = "width"
               )
               width: Int,

               @DescribeParameter(
                 name = "height",
                 description = "height"
               )
               height: Int
               ): GridCoverage2D = {

    val ff = CommonFactoryFinder.getFilterFactory2
    import org.locationtech.geomesa.utils.geotools.Conversions._
    val responseFeatures = response.features().toArray
    val collection = DataUtilities.collection(responseFeatures)
    val bounds = DataUtilities.bounds(collection)
    val Array(lx,ly) = bounds.getLowerCorner.getCoordinate
    val Array(ux,uy) = bounds.getUpperCorner.getCoordinate
    val dx = ux - lx
    val dy = uy - ly
    val bufferedBounds = JTS.toGeometry(bounds).buffer(math.max(dx,dy)/100.0).getEnvelopeInternal
    val densityBounds = JTS.toGeographic(bufferedBounds, DefaultGeographicCRS.WGS84).asInstanceOf[ReferencedEnvelope]
    val fdProcess = new FeatureDistanceProcess()
    val coverages = featureNames.flatMap { feature =>
      val Array(ns, fn) = feature.split(":")
      val fInfo = catalog.getFeatureTypeByName(ns, fn)
      val ds = fInfo.getStore.getDataStore(null)
      val fs = ds.getFeatureSource(new NameImpl(fn)).asInstanceOf[SimpleFeatureSource]
      val geomProp = fs.getSchema.getGeometryDescriptor.getLocalName
      val q = ff.bbox(ff.property(geomProp), densityBounds)
      val features = fs.getFeatures(q)
      if(features.size() == 0) None
      else Some((feature, fdProcess.execute(features, densityBounds, width, height)))
    }

    val numAttrs = 1 + coverages.size
    val attributes = coverages.map { case (fn, _) => new Attribute(fn) }
    val fv = new FastVector(numAttrs)
    val instances = new Instances("data", fv, 0)
    val classFv = new FastVector(2)
    classFv.addElement("0")
    classFv.addElement("1")
    fv.addElement(new Attribute("class", classFv))
    attributes.foreach(fv.addElement(_))
    instances.setClassIndex(0)

    responseFeatures.foreach { f =>
      val loc = f.point
      val x = loc.getX
      val y = loc.getY
      val pos = new DirectPosition2D(x, y)
      val vec = coverages.map { case (_, c) => c.evaluate(pos).asInstanceOf[Array[Float]].head }
      val inst = new Instance(numAttrs)
      inst.setValue(0, 1.0)
      vec.zipWithIndex.foreach { case (v, idx) => inst.setValue(idx+1, v.toDouble) }
      instances.add(inst)
    }
    List.fill(1000)((lx+Random.nextDouble()*dx, ly+Random.nextDouble()*dy)).foreach { case (x,y) =>
      val pos = new DirectPosition2D(x, y)
      val vec = coverages.map { case (_, c) => c.evaluate(pos).asInstanceOf[Array[Float]].head }
      val inst = new Instance(numAttrs)
      inst.setValue(0, 0.0)
      vec.zipWithIndex.foreach { case (v, idx) => inst.setValue(idx+1, v.toDouble) }
      instances.add(inst)
    }

    val classifier = new Logistic
    classifier.buildClassifier(instances)

    val gt = new GridSnap(bounds, width, height)
    var max = 0.0d
    val predictions =
      (0 until width).map { i =>
        val x = gt.x(i)
        (0 until height).map { j =>
          val y = gt.y(j)
          val pos = new DirectPosition2D(x, y)
          val vec = coverages.map { case (_, c) => c.evaluate(pos).asInstanceOf[Array[Float]].head }
          val inst = new Instance(numAttrs)
          inst.setValue(0, 0.0)
          vec.zipWithIndex.foreach { case (v, idx) => inst.setValue(idx+1, v.toDouble) }
          inst.setDataset(instances)
          val res = classifier.distributionForInstance(inst)(1)
          if(res > max) max = res
          res
        }.toArray
      }.toArray

    // rank
    val ranker = new NaturalRanking(TiesStrategy.MAXIMUM)
    val norm = (width * height).toFloat
    val ranked = ranker.rank(predictions.flatten).map { _.toFloat/norm }
    val regridded = ranked.grouped(predictions.head.size).toArray

    val gcf = CoverageFactoryFinder.getGridCoverageFactory(GeoTools.getDefaultHints)
    gcf.create("Process Results", GridUtils.flipXY(regridded), densityBounds)
  }
}

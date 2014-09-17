package org.locationtech.geomesa.plugin.process

import java.util
import java.util.Locale

import org.apache.commons.math3.stat.ranking.{NaturalRanking, TiesStrategy}
import org.geoserver.catalog.Catalog
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.DataUtilities
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.factory.GeoTools
import org.geotools.geometry.DirectPosition2D
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.referencing.CRS
import org.locationtech.geomesa.plugin.wps.GeomesaProcess
import org.locationtech.geomesa.utils.geotools.GridSnap
import org.opengis.feature.simple.SimpleFeature
import weka.classifiers.Classifier
import weka.classifiers.functions.Logistic
import weka.core.{Attribute, FastVector, Instance, Instances}

import scala.collection.JavaConversions._
import scala.util.Random


@DescribeProcess(
  title = "Discrete Choice Model",
  description = "Prediction"
)
class DCMProcess(val catalog: Catalog) extends GeomesaProcess {

  // lookup WGS84 rather than using DefaultGeographicCRS.WGS84 because
  // the Import process requires that the CRS have non-empty Identifiers
  private val WGS84 = CRS.decode("EPSG:4326")

  @DescribeResult(
    name = "prediction",
    `type` = classOf[GridCoverage2D],
    description = "Prediction")
  def execute(
               @DescribeParameter(
                 name = "predictiveFeatures",
                 collectionType = classOf[SimpleFeatureCollection],
                 description = "Predictive Features"
               )
               featureCollections: util.Collection[SimpleFeatureCollection],

               @DescribeParameter(
                 name = "predictiveCoverages",
                 collectionType = classOf[GridCoverage2D],
                 description = "Processed Predictive Features"
               )
               inputCoverages: util.Collection[GridCoverage2D],

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
               height: Int,

               @DescribeParameter(
                 name = "sampleRatio",
                 description = "Number of grid points to sample per true positive response",
                 defaultValue = "100"
               )
               sampleRatio: Int

               ): GridCoverage2D = {

    import org.locationtech.geomesa.utils.geotools.Conversions._

    val responseFeatures = response.features().toArray
    val collection = DataUtilities.collection(responseFeatures)
    val bounds = DataUtilities.bounds(collection)
    val Array(lx,ly) = bounds.getLowerCorner.getCoordinate
    val Array(ux,uy) = bounds.getUpperCorner.getCoordinate
    val dx = ux - lx
    val dy = uy - ly
    val bufferedBounds = JTS.toGeometry(bounds).buffer(math.max(dx,dy)/100.0)
    val densityBounds = new ReferencedEnvelope(bufferedBounds.getEnvelopeInternal, WGS84)

    val processedCoverages = processFeatureCollections(featureCollections, width, height, densityBounds)
    val coverages = (inputCoverages.map { gc => (gc.getName.toString(Locale.getDefault), gc) } ++ processedCoverages).toMap

    val numAttrs = 1 + coverages.size
    val instances = buildInstances(coverages, numAttrs)

    val responseInstances = processResponses(responseFeatures, numAttrs, coverages)
    responseInstances.foreach { i => instances.add(i) }
    val nullGrid = processGridSample(sampleRatio*responseFeatures.length, lx, ly, dx, dy, coverages, numAttrs)
    nullGrid.foreach { i => instances.add(i) }

    val classifier = new Logistic
    classifier.buildClassifier(instances)

    val predictions = predict(width, height, bounds, coverages, numAttrs, instances, classifier)

    val ranked = rank(width, height, predictions)

    val gcf = CoverageFactoryFinder.getGridCoverageFactory(GeoTools.getDefaultHints)
    gcf.create("Process Results", GridUtils.flipXY(ranked), densityBounds)
  }

  def processFeatureCollections(featureCollections: util.Collection[SimpleFeatureCollection],
                                width: Int,
                                height: Int,
                                densityBounds: ReferencedEnvelope): Iterable[(String, GridCoverage2D)] = {
    if(featureCollections == null || featureCollections.size() == 0) Seq()
    else {
      val fdProcess = new FeatureDistanceProcess()
      featureCollections.flatMap { features =>
        if (features.size() == 0) None
        else Some((features.getSchema.getTypeName, fdProcess.execute(features, densityBounds, width, height)))
      }
    }
  }

  def rank(width: Int, height: Int, predictions: Array[Array[Double]]): Array[Array[Float]] = {
    val ranker = new NaturalRanking(TiesStrategy.MAXIMUM)
    val norm = (width * height).toFloat
    val ranked = ranker.rank(predictions.flatten).map { _.toFloat / norm }
    ranked.grouped(predictions.head.size).toArray
  }

  def predict(width: Int, height: Int,
              bounds: ReferencedEnvelope,
              coverages: Map[String, GridCoverage2D],
              numAttrs: Int,
              instances: Instances,
              classifier: Classifier): Array[Array[Double]] = {
    val gt = new GridSnap(bounds, width, height)
    var max = 0.0d
    (0 until width).map { i =>
      val x = gt.x(i)
      (0 until height).map { j =>
        val y = gt.y(j)
        val res = predictXY(coverages, numAttrs, instances, classifier, x, y)
        if (res > max) max = res
        res
      }.toArray
    }.toArray
  }

  def predictXY(coverages: Map[String, GridCoverage2D],
                numAttrs: Int,
                instances: Instances,
                classifier: Classifier,
                x: Double, y: Double): Double = {
    val pos = new DirectPosition2D(x, y)
    val vec = coverages.map { case (_, c) => c.evaluate(pos).asInstanceOf[Array[Float]].head}
    val inst = new Instance(numAttrs)
    inst.setValue(0, 0.0)
    vec.zipWithIndex.foreach { case (v, idx) => inst.setValue(idx + 1, v.toDouble)}
    inst.setDataset(instances)
    classifier.distributionForInstance(inst)(1)
  }

  def processGridSample(count: Int,
                        lx: Double, ly: Double, dx: Double, dy: Double,
                        coverages: Map[String, GridCoverage2D],
                        numAttrs: Int): Seq[Instance] =
    List.fill(count)((lx + Random.nextDouble() * dx, ly + Random.nextDouble() * dy)).map { case (x, y) =>
      val pos = new DirectPosition2D(x, y)
      val vec = coverages.map { case (_, c) => c.evaluate(pos).asInstanceOf[Array[Float]].head}
      val inst = new Instance(numAttrs)
      inst.setValue(0, 0.0)
      vec.zipWithIndex.foreach { case (v, idx) => inst.setValue(idx + 1, v.toDouble)}
      inst
    }

  def processResponses(responseFeatures: Array[SimpleFeature],
                       numAttrs: Int,
                       coverages: Map[String, GridCoverage2D]): Seq[Instance] =
    responseFeatures.map { f => buildInstance(numAttrs, coverages, f) }

  def buildInstance(numAttrs: Int, coverages: Map[String, GridCoverage2D], f: SimpleFeature): Instance = {
    import org.locationtech.geomesa.utils.geotools.Conversions._
    val loc = f.point
    val x = loc.getX
    val y = loc.getY
    val pos = new DirectPosition2D(x, y)
    val vec = coverages.map { case (_, c) => c.evaluate(pos).asInstanceOf[Array[Float]].head}
    val inst = new Instance(numAttrs)
    inst.setValue(0, 1.0)
    vec.zipWithIndex.foreach { case (v, idx) => inst.setValue(idx + 1, v.toDouble)}
    inst
  }

  def buildInstances(coverages: Map[String, GridCoverage2D], numAttrs: Int): Instances = {
    val attributes = coverages.map { case (fn, _) => new Attribute(fn)}
    val fv = new FastVector(numAttrs)
    val instances = new Instances("data", fv, 0)
    val classFv = new FastVector(2)
    classFv.addElement("0")
    classFv.addElement("1")
    fv.addElement(new Attribute("class", classFv))
    attributes.foreach(fv.addElement(_))
    instances.setClassIndex(0)
    instances
  }
}

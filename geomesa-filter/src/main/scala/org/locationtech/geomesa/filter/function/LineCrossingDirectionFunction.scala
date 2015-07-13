package org.locationtech.geomesa.filter.function

import com.vividsolutions.jts.algorithm.RobustLineIntersector
import com.vividsolutions.jts.geom.{Geometry, LineString}
import com.vividsolutions.jts.geomgraph.GeometryGraph
import com.vividsolutions.jts.linearref.LocationIndexedLine
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._

class LineCrossingDirectionFunction
  extends FunctionExpressionImpl(
    new FunctionNameImpl(
      "lineCrossingDirection",
      classOf[java.lang.Integer],
      parameter("lineA", classOf[Geometry]),
      parameter("lineB", classOf[Geometry])
    )
  ) {

  override def evaluate(o: java.lang.Object): AnyRef = {
    val l = getExpression(0).evaluate(o).asInstanceOf[LineString]
    val r = getExpression(1).evaluate(o).asInstanceOf[LineString]
    val gl = new GeometryGraph(0, l)
    val gr = new GeometryGraph(1, r)
    val segmentIntersector = gl.computeEdgeIntersections(gr, new RobustLineIntersector(), true)
    if(segmentIntersector.hasProperIntersection) {
      // determine direction of intersection
      val intersection = segmentIntersector.getProperIntersectionPoint
      val nl = gl.find(intersection)
      val nr = gr.find(intersection)
      val lill = new LocationIndexedLine(l)
      val lilr = new LocationIndexedLine(r)
      val lx = lill.indexOf(intersection)
      val rx = lilr.indexOf(intersection)
      val segL = lx.getSegment(l)
      val segR = rx.getSegment(r)
      segL.orientationIndex()
    }
    0: java.lang.Integer
  }
}

package org.locationtech.geomesa.analytic.storm

import java.io.File

import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.codec.binary.Hex
import org.locationtech.geomesa.utils.text.WKTUtils

object SiteGeomMapping {

  def read(f: File): Seq[Site] =
    scala.io.Source.fromFile(f).getLines().map { line =>
      val arr = line.split(",").map { s => if (s.startsWith("\"")) s.drop(1).dropRight(1) else s }
      Site(arr(0), WKTUtils.read(arr(1)))
    }.toSeq
  
  case class Site(name: String, geom: Geometry)
  
  def encode(sites: Seq[Site]): String = 
    sites.map { site => 
      new String(Hex.encodeHex(site.name.getBytes)) + "," + new String(Hex.encodeHex(WKTUtils.write(site.geom).getBytes))
    }.mkString("#")
  
  def decode(s: String): Seq[Site] =
    s.split("#").map { pair => 
      val arr = pair.split(",")
      val name = new String(Hex.decodeHex(arr(0).toCharArray))
      val geom = WKTUtils.read(new String(Hex.decodeHex(arr(1).toCharArray)))
      new Site(name, geom)
    }.toSeq
}

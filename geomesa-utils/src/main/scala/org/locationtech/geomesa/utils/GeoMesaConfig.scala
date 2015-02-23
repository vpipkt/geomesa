/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils

import java.io.InputStreamReader

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.Logging

import scala.util.Try

trait GeoMesaConfig extends Logging {

  val file = "geomesa.conf"

  val defaults = ConfigFactory.parseResources(getClass, file)

  val config = Option(getClass.getClassLoader.getResource(file)).flatMap { resource =>
    val stream = Try(resource.openStream())
    val reader = stream.flatMap(s => Try(new InputStreamReader(s)))
    val config = reader.flatMap { r =>
      logger.info(s"Loading configuration file ${resource.toURI}")
      Try(ConfigFactory.parseReader(r).withFallback(defaults))
    }
    stream.foreach(s => Try(s.close()))
    reader.foreach(r => Try(r.close()))
    config.toOption
  }.getOrElse(defaults)
}

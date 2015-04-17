# GeoMesa Stream Processing

The GeoMesa Stream library provides tools to process streams of
```SimpleFeatures```.  The library can be used to instantiate a
```DataStore``` either in GeoServer or in a user's application to serve
as a constant source of ```SimpleFeatures```.  For example, you
can instantiate a ```DataStore``` that will connect to Twitter and
show the most recent tweets in a spatial context.  The timeout for
the ```DataStore``` is configurable.  A stream can be defined against
any source that can be processed by Apache Camel.  A ```SimpleFeatureConverter```
can be attached to the stream to translate the underlying data into
```SimpleFeatures```.

## Modules
 * ```geomesa-stream-api``` - the stream source and processing APIs
 * ```geomesa-stream-generic``` - definition of the Camel generic source
 * ```geomesa-stream-datastore``` - ```DataStore``` implementation
 * ```geomesa-geoserver-plugin``` - GeoServer hooks for stream sources
 
## Usage

To illustrate usage, assume we are processing a stream of Twitter data
as a csv.  The configuration in GeoServer is as follows:

```javascript
   {
     type         = "generic"
     source-route = "netty4:tcp://localhost:5899?textline=true"
     sft          = {
                      type-name = "testdata"
                      fields = [
                        { name = "label",     type = "String" }
                        { name = "geom",      type = "Point",  index = true, srid = 4326, default = true }
                        { name = "dtg",       type = "Date",   index = true }
                      ]
                    }
     threads      = 4
     converter    = {
                      id-field = "md5(string2bytes($0))"
                      type = "delimited-text"
                      format = "DEFAULT"
                      fields = [
                        { name = "label",     transform = "trim($1)" }
                        { name = "geom",      transform = "point($2::double, $3::double)" }
                        { name = "dtg",       transform = "datetime($4)" }
                      ]
                    }
   }
```   

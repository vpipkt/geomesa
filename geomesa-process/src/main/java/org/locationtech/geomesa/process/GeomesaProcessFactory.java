package org.locationtech.geomesa.process;

import org.geoserver.wps.jts.SpringBeanProcessFactory;
import org.slf4j.LazyLogging;
import org.slf4j.LazyLoggingFactory;

public class GeomesaProcessFactory extends SpringBeanProcessFactory {
    private LazyLogging log = LazyLoggingFactory.getLazyLogging(GeomesaProcessFactory.class);

    public GeomesaProcessFactory(String title, String namespace, Class markerInterface) {
        super(title, namespace, markerInterface);
        log.info("Created GeomesaProcessFactory");
    }
}

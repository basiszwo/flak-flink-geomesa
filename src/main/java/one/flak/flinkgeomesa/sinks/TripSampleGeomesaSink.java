// Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
// See LICENSE file in the project root for full license information.
package one.flak.flinkgeomesa.sinks;


import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.Geometry;
import one.flak.flinkgeomesa.models.TripSample;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.List;

public class TripSampleGeomesaSink extends BaseGeomesaSink<TripSample> {

    public TripSampleGeomesaSink(String zookeepers, String instanceId, String user, String password, String table, String simpleFeatureTypeName) {
        super(zookeepers, instanceId, user, password, table, simpleFeatureTypeName);
    }

    @Override
    protected SimpleFeatureType createSimpleFeatureType(String simpleFeatureTypeName) {
        // list the attributes that constitute the feature type
        List<String> attributes = Lists.newArrayList(
            "OccuredAt:Date",
            "TripIdentifier:String",
            "*SamplePosition:Point:srid=4326",
            "AccelerationZ:Float"
        );

        // create the bare simple-feature type
        String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
        SimpleFeatureType simpleFeatureType = SimpleFeatureTypes.createType(simpleFeatureTypeName, simpleFeatureTypeSchema);

        // use the user-data (hints) to specify which date-time field is meant to be indexed;
        // if you skip this step, your data will still be stored, it simply won't be indexed
        simpleFeatureType.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "OccuredAt");

        return simpleFeatureType;
    }

    @Override
    SimpleFeature buildSimpleFeature(TripSample tripSample) {
        final Object[] emptyFeature = {};

        SimpleFeature simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType, emptyFeature, null);

        simpleFeature.setAttribute("OccuredAt", tripSample.getOccuredAt());
        simpleFeature.setAttribute("TripIdentifier", tripSample.getTripIdentifier());
        simpleFeature.setAttribute("AccelerationZ", tripSample.getAccelerationZ());

        Geometry geometry = WKTUtils.read("POINT(" + tripSample.getLatitude() + " " + tripSample.getLongitude() + ")");
        simpleFeature.setAttribute("SamplePosition", geometry);

        return simpleFeature;
    }
}

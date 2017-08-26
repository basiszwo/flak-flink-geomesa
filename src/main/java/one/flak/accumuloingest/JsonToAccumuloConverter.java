// Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
// See LICENSE file in the project root for full license information.
package one.flak.accumuloingest;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.vividsolutions.jts.geom.Geometry;
import one.flak.accumulosetup.AccumuloConfig;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.joda.time.DateTime;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;


public class JsonToAccumuloConverter {

    public static void main(String[] args) throws Exception {
        System.out.println("READ CMDLINE OPTIONS");
        CommandLine cmd = getCmdLineOptions(args);

        System.out.println("ACCESSING DATA STORE");
        DataStore dataStore = getAccumuloDataStore(cmd);

        String filename = cmd.getOptionValue("filePath", "/Volumes/WD 2TB NEU/Diplomarbeit/samples-converted/20170219/1386035.json");

        JsonArray tripSamples = FileHelpers.readJsonFromFile(filename).getAsJsonArray();

        // establish specifics concerning the SimpleFeatureType to store
        SimpleFeatureType simpleFeatureType = AccumuloConfig.createSimpleFeatureType(AccumuloConfig.SIMPLE_FEATURE_TYPE_NAME);

        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        Object[] emptyFeature = {};

        // we simply assign a random UUID as trip identifier
        String tripIdentifier = String.valueOf(UUID.randomUUID());

        for(JsonElement sample : tripSamples) {
//            featureCollection = new DefaultFeatureCollection();

            SimpleFeature simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType, emptyFeature, null);

            try {
                JsonObject json = sample.getAsJsonObject();
                long rawTimestamp = (long)(json.get("timestamp").getAsDouble() * 1000);
                double latitude = Double.parseDouble(json.get("latitude").getAsString());
                double longitude = Double.parseDouble(json.get("longitude").getAsString());
                double accelerationZ = Double.parseDouble(json.get("accl_z").getAsString());

                simpleFeature.setAttribute("OccuredAt", new DateTime().withMillis(rawTimestamp).toDate());
                simpleFeature.setAttribute("TripIdentifier", tripIdentifier);
                simpleFeature.setAttribute("AccelerationZ", accelerationZ);

                Geometry geometry = WKTUtils.read("POINT(" + latitude + " " + longitude + ")");
                simpleFeature.setAttribute("SamplePosition", geometry);

                featureCollection.add(simpleFeature);

                System.out.println(
                        "TripId " + tripIdentifier + " | " +
                                "OccuredAt " + new Date(rawTimestamp) + " | " +
                                "Position " + latitude + " / " + longitude + " | " +
                                "AccelerationZ " + accelerationZ + " | "
                );
            } catch (UnsupportedOperationException name) {
                System.out.println("JSON parse error ... skipping");
            }
        }

        System.out.printf("INSERTING %d samples ...", tripSamples.size());
        insertFeatures(AccumuloConfig.SIMPLE_FEATURE_TYPE_NAME, dataStore, featureCollection);
        System.out.printf("DONE INSERTING %d samples ...", tripSamples.size());

//        System.out.println("Reading some values ...");
//
//        Filter filter = CQL.toFilter("INCLUDE");
//
//        Query query = new Query(AccumuloConfig.SIMPLE_FEATURE_TYPE_NAME, filter);
//        FeatureSource source = dataStore.getFeatureSource(AccumuloConfig.SIMPLE_FEATURE_TYPE_NAME);
//
//        FeatureIterator features = source.getFeatures(query).features();
//
//
//        while(features.hasNext()) {
//            Feature feature = features.next();
//
//            System.out.println(
//                    "TripId " + feature.getProperty("TripIdentifier").getValue() + " | " +
//                            "OccuredAt " + feature.getProperty("OccuredAt").getValue() + " | " +
//                            "Position " + feature.getProperty("SamplePosition").getValue() + " | " +
//                            "AccelerationZ " + feature.getProperty("AccelerationZ").getValue() + " | "
//            );
//
//        }
//        features.close();
//        dataStore.dispose();
    }


    static void insertFeatures(String simpleFeatureTypeName,
                               DataStore dataStore,
                               FeatureCollection featureCollection)
            throws IOException {

        FeatureStore featureStore = (SimpleFeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);

        featureStore.addFeatures(featureCollection);
    }

    static CommandLine getCmdLineOptions(String[] args) throws Exception {
        // find out where -- in Accumulo -- the user wants to store data
        CommandLineParser parser = new BasicParser();
        Options options = AccumuloConfig.getCommonRequiredOptions();
        CommandLine cmd = parser.parse( options, args);

        return cmd;
    }

    static DataStore getAccumuloDataStore(CommandLine cmd) throws Exception {
        // verify that we can see this Accumulo destination in a GeoTools manner
        Map<String, String> dsConf = AccumuloConfig.getAccumuloDataStoreConf(cmd);
        DataStore dataStore = DataStoreFinder.getDataStore(dsConf);

        assert dataStore != null;

        return dataStore;
    }

}

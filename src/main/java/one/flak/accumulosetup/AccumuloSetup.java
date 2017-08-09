package one.flak.accumulosetup;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.Map;

import static one.flak.accumulosetup.AccumuloConfig.*;


public class AccumuloSetup {

    public static void main(String[] args) throws Exception {
        // find out where -- in Accumulo -- the user wants to store data
        CommandLineParser parser = new BasicParser();
        Options options = getCommonRequiredOptions();
        CommandLine cmd = parser.parse( options, args);

        // verify that we can see this Accumulo destination in a GeoTools manner
        Map<String, String> dsConf = getAccumuloDataStoreConf(cmd);
        DataStore dataStore = DataStoreFinder.getDataStore(dsConf);
        assert dataStore != null;

        // establish specifics concerning the SimpleFeatureType to store
        String simpleFeatureTypeName = "FlakAcceleration";
        SimpleFeatureType simpleFeatureType = createSimpleFeatureType(SIMPLE_FEATURE_TYPE_NAME);

        // write Feature-specific metadata to the destination table in Accumulo
        // (first creating the table if it does not already exist); you only need
        // to create the FeatureType schema the *first* time you write any Features
        // of this type to the table
        System.out.println("Creating feature-type (schema):  " + SIMPLE_FEATURE_TYPE_NAME);
        dataStore.createSchema(simpleFeatureType);

        dataStore.dispose();
    }

}

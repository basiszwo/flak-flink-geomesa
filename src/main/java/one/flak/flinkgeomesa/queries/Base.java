// Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
// See LICENSE file in the project root for full license information.

package one.flak.flinkgeomesa.queries;

import one.flak.accumulosetup.AccumuloConfig;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.Query;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.util.Map;

public class Base {

    public static void main(String[] args) throws Exception {
        System.out.println("READ CMDLINE OPTIONS ...");
        CommandLine cmd = getCmdLineOptions(args);

        System.out.println("ACCESSING DATA STORE ...");
        DataStore dataStore = getAccumuloDataStore(cmd);

        System.out.println("Executing query ...");

        Filter filter = CQL.toFilter("INCLUDE");

        Query query = new Query(AccumuloConfig.SIMPLE_FEATURE_TYPE_NAME, filter);
        query.setMaxFeatures(100);

        FeatureSource source = dataStore.getFeatureSource(AccumuloConfig.SIMPLE_FEATURE_TYPE_NAME);
        FeatureIterator features = source.getFeatures(query).features();


        while(features.hasNext()) {
            Feature feature = features.next();

            System.out.println(
                "TripId: " + feature.getProperty("TripIdentifier").getValue() + " | " +
                "OccuredAt: " + feature.getProperty("OccuredAt").getValue() + " | " +
                "Position: " + feature.getProperty("SamplePosition").getValue() + " | " +
                "AccelerationZ: " + feature.getProperty("AccelerationZ").getValue() + " | "
            );

        }
        features.close();
        dataStore.dispose();

        System.out.println("DONE");

        System.exit(0);
    }


    static Filter createFilter(String geomField, double x0, double y0, double x1, double y1,
                               String dateField, String t0, String t1,
                               String attributesQuery)
            throws CQLException, IOException {

        // there are many different geometric predicates that might be used;
        // here, we just use a bounding-box (BBOX) predicate as an example.
        // this is useful for a rectangular query area
        String cqlGeometry = "BBOX(" + geomField + ", " +
                x0 + ", " + y0 + ", " + x1 + ", " + y1 + ")";

        // there are also quite a few temporal predicates; here, we use a
        // "DURING" predicate, because we have a fixed range of times that
        // we want to query
        String cqlDates = "(" + dateField + " DURING " + t0 + "/" + t1 + ")";

        // there are quite a few predicates that can operate on other attribute
        // types; the GeoTools Filter constant "INCLUDE" is a default that means
        // to accept everything
        String cqlAttributes = attributesQuery == null ? "INCLUDE" : attributesQuery;

        String cql = cqlGeometry + " AND " + cqlDates  + " AND " + cqlAttributes;
        return CQL.toFilter(cql);
    }

    static void queryFeatures(String simpleFeatureTypeName,
                              DataStore dataStore,
                              String geomField, double x0, double y0, double x1, double y1,
                              String dateField, String t0, String t1,
                              String attributesQuery)
            throws CQLException, IOException {

        // construct a (E)CQL filter from the search parameters,
        // and use that as the basis for the query
        Filter cqlFilter = createFilter(geomField, x0, y0, x1, y1, dateField, t0, t1, attributesQuery);
        Query query = new Query(simpleFeatureTypeName, cqlFilter);

        // submit the query, and get back an iterator over matching features
        FeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeName);
        FeatureIterator featureItr = featureSource.getFeatures(query).features();

        // loop through all results
        int n = 0;
        while (featureItr.hasNext()) {
            Feature feature = featureItr.next();
            System.out.println((++n) + ".  " +
                    feature.getProperty("Who").getValue() + "|" +
                    feature.getProperty("What").getValue() + "|" +
                    feature.getProperty("When").getValue() + "|" +
                    feature.getProperty("Where").getValue() + "|" +
                    feature.getProperty("Why").getValue());
        }
        featureItr.close();
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

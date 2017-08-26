// Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
// See LICENSE file in the project root for full license information.

package one.flak.accumuloingest;

import com.vividsolutions.jts.geom.Geometry;
import one.flak.accumulosetup.AccumuloConfig;
import one.flak.flinkgeomesa.models.TripSample;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureStore;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.joda.time.DateTime;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class CsvAccumuloIngest {

    public static void main(String[] args) throws MalformedURLException, URISyntaxException, SchemaException {
        if(args.length != 1) {
            System.out.println("You have to provide a configuration file as argument");
            System.exit(1);
        }
        Properties props = new Properties();


        try(BufferedReader reader = Files.newBufferedReader(new File(args[0]).toPath())) {
            props.load(reader);
        } catch(IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }


        int startDate = Integer.parseInt(props.getProperty("startDate"));
        int endDate = Integer.parseInt(props.getProperty("endDate"));
        File inputFolder = new File(props.getProperty("inputFolder"));


        List<File> directories = Arrays.asList(inputFolder.listFiles())
                .stream()
                .filter((e) -> {
                    if(e.getName().equals(".DS_Store")) {
                        return false;
                    }

                    int fileDate = Integer.parseInt(FilenameUtils.getBaseName(e.getName()));

                    boolean isInRange = fileDate >= startDate && fileDate <= endDate;

                    return isInRange && e.isDirectory();
                })
                .collect(Collectors.toList());

        System.out.println("ACCESSING DATA STORE");
        DataStore dataStore = null;

        try {
            dataStore = getAccumuloDataStore(props);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        // establish specifics concerning the SimpleFeatureType to store
        SimpleFeatureType simpleFeatureType = AccumuloConfig.createSimpleFeatureType(AccumuloConfig.SIMPLE_FEATURE_TYPE_NAME);

        Object[] emptyFeature = {};

        DataStore finalDataStore1 = dataStore;

        directories.parallelStream().forEach((dir) -> {
            DataStore finalDataStore = finalDataStore1;

            Arrays.asList(dir.listFiles()).parallelStream()
                .filter((file) -> {
                    if(file.getName().equals(".DS_Store")) {
                        return false;
                    }

                    return !file.isDirectory();
                })
                .forEach((f) -> {
                    System.out.println("Processing file " + f.getAbsolutePath());

                    DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

                    TripSampleCsvReader tripSampleCsvReader = null;
                    try {
                        tripSampleCsvReader = new TripSampleCsvReader(f.toString());
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }

                    int counter = 0;

                    while(tripSampleCsvReader.hasNext()) {
                        counter++;
                        try {
                            TripSample sample= tripSampleCsvReader.next();

                            SimpleFeature simpleFeature = SimpleFeatureBuilder.build(simpleFeatureType, emptyFeature, null);

                            simpleFeature.setAttribute("OccuredAt", sample.getOccuredAt());
                            simpleFeature.setAttribute("TripIdentifier", sample.getTripIdentifier());
                            simpleFeature.setAttribute("AccelerationZ", sample.getAccelerationZ());

                            Geometry geometry = WKTUtils.read("POINT(" + Double.toString(sample.getLatitude())+ " " + Double.toString(sample.getLongitude()) + ")");

                            simpleFeature.setAttribute("SamplePosition", geometry);

                            featureCollection.add(simpleFeature);

                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        if(counter == 2500) {
                            System.out.println("INSERTING samples ...");
                            try {
                                insertFeatures(AccumuloConfig.SIMPLE_FEATURE_TYPE_NAME, finalDataStore, featureCollection);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            System.out.println("DONE INSERTING samples ...");
                            counter = 0;

                            featureCollection.clear();
                        }

                    }


                    System.out.println("INSERTING samples ...");
                    try {
                        insertFeatures(AccumuloConfig.SIMPLE_FEATURE_TYPE_NAME, finalDataStore, featureCollection);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println("DONE INSERTING samples ...");
                });
        });

        System.out.println("DONE DONE");

    };

    static void insertFeatures(String simpleFeatureTypeName,
                               DataStore dataStore,
                               FeatureCollection featureCollection)
            throws IOException {

        FeatureStore featureStore = (SimpleFeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);

        featureStore.addFeatures(featureCollection);
    }

    static DataStore getAccumuloDataStore(Properties props) throws Exception {
        // verify that we can see this Accumulo destination in a GeoTools manner
        Map<String, String> dsConf = AccumuloConfig.getAccumuloDataStoreConf(props);
        DataStore dataStore = DataStoreFinder.getDataStore(dsConf);

        assert dataStore != null;

        return dataStore;
    }

    static CommandLine getCmdLineOptions(String[] args) throws Exception {
        // find out where -- in Accumulo -- the user wants to store data
        CommandLineParser parser = new BasicParser();
        Options options = AccumuloConfig.getCommonRequiredOptions();
        CommandLine cmd = parser.parse( options, args);

        return cmd;
    }
}

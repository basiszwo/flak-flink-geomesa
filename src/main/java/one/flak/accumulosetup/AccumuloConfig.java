package one.flak.accumulosetup;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.geotools.feature.SchemaException;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AccumuloConfig {
    static final String INSTANCE_ID = "instanceId";
    static final String ZOOKEEPERS = "zookeepers";
    static final String USER = "user";
    static final String PASSWORD = "password";
    static final String AUTHS = "auths";
    static final String TABLE_NAME = "tableName";

    public static final String SIMPLE_FEATURE_TYPE_NAME = "FlakAcceleration";

    // sub-set of parameters that are used to create the Accumulo DataStore
    static final String[] ACCUMULO_CONNECTION_PARAMS = new String[] {
            INSTANCE_ID,
            ZOOKEEPERS,
            USER,
            PASSWORD,
            AUTHS,
            TABLE_NAME
    };

    /**
     * Creates a common set of command-line options for the parser.  Each option
     * is described separately.
     */
    public static Options getCommonRequiredOptions() {
        Options options = new Options();

        Option instanceIdOpt = OptionBuilder.withArgName(INSTANCE_ID)
                .hasArg()
                .isRequired()
                .withDescription("the ID (name) of the Accumulo instance, e.g:  mycloud")
                .create(INSTANCE_ID);
        options.addOption(instanceIdOpt);

        Option zookeepersOpt = OptionBuilder.withArgName(ZOOKEEPERS)
                .hasArg()
                .isRequired()
                .withDescription("the comma-separated list of Zookeeper nodes that support your Accumulo instance, e.g.:  zoo1:2181,zoo2:2181,zoo3:2181")
                .create(ZOOKEEPERS);
        options.addOption(zookeepersOpt);

        Option userOpt = OptionBuilder.withArgName(USER)
                .hasArg()
                .isRequired()
                .withDescription("the Accumulo user that will own the connection, e.g.:  root")
                .create(USER);
        options.addOption(userOpt);

        Option passwordOpt = OptionBuilder.withArgName(PASSWORD)
                .hasArg()
                .isRequired()
                .withDescription("the password for the Accumulo user that will own the connection, e.g.:  toor")
                .create(PASSWORD);
        options.addOption(passwordOpt);

        Option authsOpt = OptionBuilder.withArgName(AUTHS)
                .hasArg()
                .withDescription("the (optional) list of comma-separated Accumulo authorizations that should be applied to all data written or read by this Accumulo user; note that this is NOT the list of low-level database permissions such as 'Table.READ', but more a series of text tokens that decorate cell data, e.g.:  Accounting,Purchasing,Testing")
                .create(AUTHS);
        options.addOption(authsOpt);

        Option tableNameOpt = OptionBuilder.withArgName(TABLE_NAME)
                .hasArg()
                .isRequired()
                .withDescription("the name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data")
                .create(TABLE_NAME);
        options.addOption(tableNameOpt);

        Option filePathOpt = OptionBuilder.withArgName("filePath")
                .hasArg()
                .withDescription("path to file")
                .create("filePath");
        options.addOption(filePathOpt);

        return options;
    }


    public static Map<String, String> getAccumuloDataStoreConf(Properties props) {
        Map<String , String> dsConf = new HashMap<String , String>();

        for (String param : ACCUMULO_CONNECTION_PARAMS) {
            dsConf.put(param, props.getProperty(param));
        }

        if (dsConf.get(AUTHS) == null) dsConf.put(AUTHS, "");

        return dsConf;
    }


    public static Map<String, String> getAccumuloDataStoreConf(CommandLine cmd) {
        Map<String , String> dsConf = new HashMap<String , String>();

        for (String param : ACCUMULO_CONNECTION_PARAMS) {
            dsConf.put(param, cmd.getOptionValue(param));
        }

        if (dsConf.get(AUTHS) == null) dsConf.put(AUTHS, "");

        return dsConf;
    }

    // Create a simple feature type for accelerations
    public static SimpleFeatureType createSimpleFeatureType(String simpleFeatureTypeName)
            throws SchemaException {

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
}

// Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
// See LICENSE file in the project root for full license information.

package one.flak.flinkgeomesa.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureStore;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public abstract class BaseGeomesaSink<TEntity> extends RichSinkFunction<TEntity> {

    // the comma-separated list of Zookeeper nodes that support your Accumulo instance
    // e.g.:  zoo1:2181,zoo2:2181,zoo3:2181
    private final String zookeepers;

    // the ID (name) of the Accumulo instance, e.g:  mycloud
    private final String instanceId;

    // the Accumulo user that will own the connection, e.g.:  root
    private final String user;

    // the password for the Accumulo user that will own the connection, e.g.:  toor
    private final String password;

    // the name of the Accumulo table to use -- or create
    // will be created if it does not exist
    private final String tableName;

    private final String simpleFeatureTypeName;

    protected SimpleFeatureType simpleFeatureType;

    // dataStore that holds connection to accumulo
    private DataStore dataStore;

    private DefaultFeatureCollection featureCollection;

    public final int BATCH_SIZE = 2500;


    public BaseGeomesaSink(String zookeepers, String instanceId, String user, String password, String tableName, String simpleFeatureTypeName) {
        this.zookeepers = zookeepers;
        this.instanceId = instanceId;
        this.user = user;
        this.password = password;
        this.tableName = tableName;
        this.simpleFeatureTypeName = simpleFeatureTypeName;
    }

    @Override
    public void invoke(TEntity entity) throws Exception {
        SimpleFeature simpleFeature = buildSimpleFeature(entity);

        insertSimpleFeature(simpleFeature);

        if(requiresFlush()) {
            flushFeatureCollection();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Map<String, String> dataStoreConf = new HashMap<String,String>();

        dataStoreConf.put("zookeepers", zookeepers);
        dataStoreConf.put("instanceId", instanceId);
        dataStoreConf.put("user", user);
        dataStoreConf.put("password", password);
        dataStoreConf.put("tableName", tableName);
        dataStoreConf.put("auths", "");

        this.dataStore = DataStoreFinder.getDataStore(dataStoreConf);
        this.simpleFeatureType = createSimpleFeatureType(simpleFeatureTypeName);
        this.featureCollection = new DefaultFeatureCollection();
    }

    @Override
    public void close() throws Exception {
        // Always flush the feature collection, so that no feature gets lost.
        // flushFeatureCollection();

        // close connection
        dataStore.dispose();
    }

    abstract SimpleFeatureType createSimpleFeatureType(String simpleFeatureTypeName) throws SchemaException;

    abstract SimpleFeature buildSimpleFeature(TEntity entity);

    private boolean requiresFlush() {
        return this.featureCollection.size() >= BATCH_SIZE;
    }

    private void insertSimpleFeature(SimpleFeature simpleFeature) {
        this.featureCollection.add(simpleFeature);
    }

    private void flushFeatureCollection() {
        FeatureStore featureStore = null;
        try {
            featureStore = (SimpleFeatureStore) dataStore.getFeatureSource(simpleFeatureTypeName);

            featureStore.addFeatures(this.featureCollection);

            featureCollection.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

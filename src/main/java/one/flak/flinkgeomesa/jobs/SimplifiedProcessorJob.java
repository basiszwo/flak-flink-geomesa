// Licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
// See LICENSE file in the project root for full license information.
package one.flak.flinkgeomesa.jobs;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import one.flak.flinkgeomesa.models.TripSample;
import one.flak.flinkgeomesa.models.TripSampleBuilder;
import one.flak.flinkgeomesa.sinks.TripSampleGeomesaSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.joda.time.DateTime;

import java.util.Date;

public class SimplifiedProcessorJob {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // parse user parameters
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("topic"),
                new SimpleStringSchema(),
                parameterTool.getProperties()
        );

        DataStream<String> messageStream = env.addSource(kafkaConsumer);

        messageStream.rebalance().map(new MapFunction<String, TripSample>() {
                                          private static final long serialVersionUID = -6867736771747690202L;

                                          @Override
                                          public TripSample map(String value) throws Exception {
                                              JsonObject json = new JsonParser().parse(value).getAsJsonObject();

                                              long rawTimestamp = json.get("created_at").getAsLong();
                                              Date occurredAt = new DateTime().withMillis(rawTimestamp).toDate();
                                              double latitude = Double.parseDouble(json.get("location").getAsJsonObject().get("latitude").getAsString());
                                              double longitude = Double.parseDouble(json.get("location").getAsJsonObject().get("longitude").getAsString());
                                              double accelerationZ = Double.parseDouble(json.get("acceleration").getAsJsonObject().get("z").getAsString());

                                              TripSample tripSample = new TripSampleBuilder()
                                                      .setOccuredAt(occurredAt)
                                                      .setAccelerationZ(accelerationZ)
                                                      .setLatitude(latitude)
                                                      .setLongitude(longitude)
                                                      .createTripSample();

                                              return tripSample;
                                          }
        }).print();

        env.execute();
    }

}

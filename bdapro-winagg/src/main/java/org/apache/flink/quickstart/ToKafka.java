package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

/**
 * Created by patrick on 22.11.16.
 */


import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;


public class ToKafka {

    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    public static final String CLEANSED_RIDES_TOPIC = "test2";

    public static void main(String[] args) throws Exception {

        // ParameterTool params = ParameterTool.fromArgs(args);
        // String input = params.getRequired("input");
        String input="/home/patrick/Dokumente/flink-java-project/nycTaxiRides.gz";

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minute are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

        DataStream<TaxiRide> filteredRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new NYCFilter());

        // write the filtered data to a Kafka sink
        filteredRides.addSink(new FlinkKafkaProducer09<>(
                LOCAL_KAFKA_BROKER,
                CLEANSED_RIDES_TOPIC,
                new TaxiRideSchema()));

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing");
    }


    public static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {

            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                    GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }

}

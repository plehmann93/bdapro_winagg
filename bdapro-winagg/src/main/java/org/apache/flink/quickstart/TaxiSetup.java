package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Lehmann on 19.11.2016.
 */
public class TaxiSetup {


    public static void main(String[] args) throws Exception {

        int maxDelay=10;
        int servingSpeed=1;
        String pathToTaxi="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\Big Data\\Flink_Project\\flink-java-project\\nycTaxiRides.gz";
        // get an ExecutionEnvironment
    StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
// configure event-time processing
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // get the taxi ride data stream
    DataStream<TaxiRide> rides = env.addSource(
            new TaxiRideSource(pathToTaxi, maxDelay, servingSpeed));

}}

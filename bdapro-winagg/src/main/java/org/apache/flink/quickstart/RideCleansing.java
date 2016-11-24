package org.apache.flink.quickstart;


        import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
        import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
        import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
        import org.apache.flink.api.common.functions.FilterFunction;
        import org.apache.flink.api.java.utils.ParameterTool;
        import org.apache.flink.streaming.api.TimeCharacteristic;
        import org.apache.flink.streaming.api.datastream.DataStream;
        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Java reference implementation for the "Ride Cleansing" exercise of the Flink training
 * (http://dataartisans.github.io/flink-training).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 *
 * Parameters:
 *   -input path-to-input-file
 *
 */
public class RideCleansing {

    public static void main(String[] args) throws Exception {

        //ParameterTool params = ParameterTool.fromArgs(args);
       // final String input = params.getRequired("input");
        String pathToTaxi="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\Big Data\\Flink_Project\\flink-java-project\\nycTaxiRides.gz";
        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(pathToTaxi, maxEventDelay, servingSpeedFactor));

        DataStream<TaxiRide> filteredRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new NYCFilter());

        // print the filtered stream
        filteredRides.print();

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
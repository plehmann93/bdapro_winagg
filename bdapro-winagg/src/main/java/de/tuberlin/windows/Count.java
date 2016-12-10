package de.tuberlin.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import de.tuberlin.io.Conf;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Lehmann on 19.11.2016.
 */
public class Count {

    public Count(Conf conf) throws Exception{


    // read parameters
        //  ParameterTool params = ParameterTool.fromArgs(args);
        // String input = params.getRequired("input");
        String pathToTaxi="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\nycTaxiRides.gz";
        final int maxEventDelay = conf.getMaxEventDelay();       // events are out of order by max 60 seconds
        final int servingSpeedFactor = conf.getServingSpeedFactor(); // events of 10 minutes are served in 1 second
        final int countNumber = conf.getCountSize();         //size of elements in each window
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(pathToTaxi, maxEventDelay, servingSpeedFactor));

        // find average number of passengers per minute starting a taxi ride
      //  DataStream<Tuple4<Double,Integer,String,String>> popularSpots = rides
       DataStream<Tuple3<Double,String,Long>> averagePassengers = rides

                //filter out those events that are not starting
                .filter(x->x.isStart)

                //just keep important variables
                .map(new Aggregations.MapToPassenger())

               //grouping all values
                .keyBy(0)
                // count window of length defined as in conf.ini
                .countWindow(countNumber)

               //sums the 1s and the passengers for the whole window
                .reduce( new Aggregations.SumAllValues())

               .map(new Aggregations.MapToMean());


        // print result on stdout
        averagePassengers.print();

        // execute the transformation pipeline
        env.execute("Flink Count");
    }







}
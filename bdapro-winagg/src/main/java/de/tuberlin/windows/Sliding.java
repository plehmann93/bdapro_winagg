package de.tuberlin.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.basics.*;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import de.tuberlin.io.Conf;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.quickstart.GetMean;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.scala.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Short;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by Lehmann on 19.11.2016.
 */
public class Sliding {




    public Sliding(Conf conf) throws Exception{

        // read parameters
        //  ParameterTool params = ParameterTool.fromArgs(args);
        // String input = params.getRequired("input");
        String pathToTaxi="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\nycTaxiRides.gz";
        final int popThreshold = 20;        // threshold for popular places
        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
        final int windowTime = conf.getWindowSize();          //measured in seconds
        final int slidingTime = conf.getWindowSlideSize();          //measured in seconds
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(pathToTaxi, maxEventDelay, servingSpeedFactor));

        // find average number of passengers per minute starting a taxi ride
        DataStream<Tuple3<Double,String,Long>> averagePassengers = rides
                //filter out those events that are not starting
                .assignTimestamps(new Aggregations.TimeStamp())
                .filter(x->x.isStart)
                //just keep important variables
                .map(new Aggregations.MapToPassenger())
                //grouping all values

                .keyBy(0)
                // tumbling time window of 1 minute length
                .timeWindow(Time.seconds(windowTime),Time.seconds(slidingTime))

               //sums the 1s and the passengers for the whole window
                .reduce( new Aggregations.SumAllValues())

                .map(new Aggregations.MapToMean());


        // print result on stdout
        averagePassengers.print();

        // execute the transformation pipeline
        env.execute("Flink Sliding");
    }





}
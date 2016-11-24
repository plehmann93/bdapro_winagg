package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.basics.*;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.scala.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Short;

import java.util.concurrent.TimeUnit;

/**
 * Created by Lehmann on 19.11.2016.
 */
public class GetMean {

    public static void main(String[] args) throws Exception {

        // read parameters
        //  ParameterTool params = ParameterTool.fromArgs(args);
        // String input = params.getRequired("input");
        String pathToTaxi="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\Big Data\\Flink_Project\\flink-java-project\\nycTaxiRides.gz";
        final int popThreshold = 20;        // threshold for popular places
        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(pathToTaxi, maxEventDelay, servingSpeedFactor));

        // find average number of passengers per minute starting a taxi ride
        DataStream<Tuple3<Double,Integer,Time>> popularSpots = rides
                // remove all rides which are not within NYC
                //  .filter(new com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansing.NYCFilter())
                //filter out those events that are not starting
                .filter(x->x.isStart)
                //just keep important variables
                .map(new GetMean.MapToPassenger())
                //grouping all values

                //TODO group by timestamp
                .keyBy(0)
                //TODO use aggregations
                // tumbling time window of 1 minute length
                .timeWindow(Time.minutes(60))
                //get average passenger in that time window
                .apply(new GetMean.RideCounter());


        // print result on stdout
        popularSpots.print();

        // execute the transformation pipeline
        env.execute("Popular Places");
    }


    /**
     Maps Taxiride so just id of ride and passengercount stays
     */
    public static class MapToPassenger implements MapFunction<TaxiRide, Tuple2<Long, Integer>> {

        @Override
        public Tuple2<Long, Integer> map(TaxiRide taxiRide) throws Exception {

            return new Tuple2<Long,Integer>(Long.valueOf(1), Integer.valueOf(taxiRide.passengerCnt));

        }
    }


    /**
     * Returns the average number of passengers in a specific time window
     */
    public static class RideCounter implements WindowFunction<
            Tuple2<Long, Integer>, // input type
            Tuple3<Double,Integer,Time>, // output type
            Tuple, // key type
            TimeWindow> // window type
    {

        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Tuple2<Long, Integer>> values,
                Collector<Tuple3<Double,Integer,Time>> out) throws Exception {

//            Long cellId = ((Tuple2<Long, Integer>)key).f0;
            //           Integer passenger = ((Tuple2<Long, Integer>)key).f1;
            long windowTime = window.getStart();
            Double cnt = 0.0;
            Double sum = 0.0;

            for(Tuple2<Long, Integer> v : values) {
                cnt += 1;
                sum += v.f1;
            }

            out.collect(new Tuple3<>(Double.valueOf( Math.round(sum/cnt*1000.0)/1000.0),cnt.intValue(), Time.hours(windowTime)));
            //out.collect(new Tuple1<>( Double.valueOf( Math.round(cnt*100.0)/100.0)));
        }
    }

    /**
     * Maps the grid cell id back to longitude and latitude coordinates.
     */
    public static class GridToCoordinates implements
            MapFunction<Tuple4<Integer, Long, Boolean, Integer>, Tuple5<Float, Float, Long, Boolean, Integer>> {

        @Override
        public Tuple5<Float, Float, Long, Boolean, Integer> map(
                Tuple4<Integer, Long, Boolean, Integer> cellCount) throws Exception {

            return new Tuple5<>(
                    GeoUtils.getGridCellCenterLon(cellCount.f0),
                    GeoUtils.getGridCellCenterLat(cellCount.f0),
                    cellCount.f1,
                    cellCount.f2,
                    cellCount.f3);
        }
    }

}
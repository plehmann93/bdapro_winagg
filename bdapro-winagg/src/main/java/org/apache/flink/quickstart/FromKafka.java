package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Properties;




import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * Created by Lehmann on 22.11.2016.
 */
public class FromKafka {

    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    private static final String RIDE_SPEED_GROUP = "rideSpeedGroup";
    //private static final String RIDE_SPEED_GROUP = "test";
    private static final int MAX_EVENT_DELAY = 60; // rides are at most 60 sec out-of-order.

    public static void main(String[] args) throws Exception {

        final int popThreshold = 20; // threshold for popular places

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // configure the Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("group.id", RIDE_SPEED_GROUP);
        // always read the Kafka topic from the start
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        // create a Kafka consumer
        FlinkKafkaConsumer09<TaxiRide> consumer = new FlinkKafkaConsumer09<>(
                "test3",
                new TaxiRideSchema(),
                kafkaProps);
        // assign a timestamp extractor to the consumer
        //consumer.assignTimestampsAndWatermarks(new TaxiRideTSExtractor());

        // create a TaxiRide data stream
        DataStream<TaxiRide> rides = env.addSource(consumer);

        // find popular places
     /*   DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> popularPlaces = rides
                // match ride to grid cell and event type (start or end)
                .map(new GridCellMatcher())
                // partition by cell id and event type
                .keyBy(0, 1)
                // build sliding window
                .timeWindow(Time.minutes(15), Time.minutes(5))
                // count ride events in window
                .apply(new RideCounter())
                // filter by popularity threshold
                .filter(new FilterFunction<Tuple4<Integer, Long, Boolean, Integer>>() {
                    @Override
                    public boolean filter(Tuple4<Integer, Long, Boolean, Integer> count) throws Exception {
                        return count.f3 >= popThreshold;
                    }
                })
                // map grid cell to coordinates
                .map(new GridToCoordinates());

        popularPlaces.print();
*/

        // find average number of passengers per minute starting a taxi ride
        DataStream<Tuple3<Double,Integer,Time>> popularSpots = rides
                // remove all rides which are not within NYC
                //  .filter(new com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansing.NYCFilter())
                //filter out those events that are not starting
                .filter(x->x.isStart)
                //just keep important variables
                .map(new FromKafka.MapToPassenger())
                //grouping all values

                //TODO group by timestamp
                .keyBy(0)
                //TODO use aggregations
                // tumbling time window of 1 minute length
                .timeWindow(Time.minutes(60))
                //get average passenger in that time window
                .apply(new FromKafka.RideCounter());


        // print result on stdout
        popularSpots.print();

        //rides.print();
        // execute the transformation pipeline
        env.execute("Popular Places from Kafka");
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




}

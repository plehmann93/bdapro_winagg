package de.tuberlin.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import de.tuberlin.io.Conf;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;


/**
 * Created by Lehmann on 22.11.2016.
 */
public class FlinkTumblingWindowFromKafka {

    private static int MAX_EVENT_DELAY=0;

    public FlinkTumblingWindowFromKafka(Conf conf) throws Exception{


        final String LOCAL_ZOOKEEPER_HOST = conf.getLocalZookeeperHost();
        final String LOCAL_KAFKA_BROKER = conf.getLocalKafkaBroker();
        final String GROUP_ID = conf.getGroupId();
        final String TOPIC_NAME=conf.getTopicName();
        //private static final String GROUP_ID = "test";

        MAX_EVENT_DELAY = conf.getMaxEventDelay();       // events are out of order by max 60 seconds
        final int servingSpeedFactor = conf.getServingSpeedFactor(); // events of 10 minutes are served in 1 second
        final int countNumber = conf.getCountSize();         //size of elements in each window
        final int windowTime = conf.getWindowSize();          //measured in seconds
        final int slidingTime = conf.getWindowSlideSize();          //measured in seconds
        final int windowType= conf.getWindowType();

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // configure the Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP_ID);
        // always read the Kafka topic from the start
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        // create a Kafka consumer
        FlinkKafkaConsumer09<TaxiRide> consumer = new FlinkKafkaConsumer09<>(
                TOPIC_NAME,
                new TaxiRideSchema(),
                kafkaProps);
        // assign a timestamp extractor to the consumer
        consumer.assignTimestampsAndWatermarks(new org.apache.flink.quickstart.FromKafka.TaxiRideTSExtractor());

        // create a TaxiRide data stream
        DataStream<TaxiRide> rides = env.addSource(consumer);


        // find average number of passengers per minute starting a taxi ride
        DataStream<Tuple3<Double,String,Long>> averagePassengers = rides

                //filter out those events that are not starting

                //.filter(x->x.isStart)

                //just keep important variables
                .map(new Aggregations.MapToPassenger())

                //grouping all values
                .keyBy(0)

                // tumbling window
                .timeWindow(Time.seconds(windowTime))

                //sums the 1s and the passengers for the whole window
            //   .reduce( new Aggregations.SumAllValues())
                  .apply(new Aggregations.PassengerCounter())      ;
             //  .map(new Aggregations.MapToMean());


        // print result on stdout
        averagePassengers.print();
        // execute the transformation pipeline
        env.execute("Windowed Aggregation from Kafka with Flink");
    }

    /**
     * Assigns timestamps to TaxiRide records.
     * Watermarks are a fixed time interval behind the max timestamp and are periodically emitted.
     */
    public static class TaxiRideTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<TaxiRide> {

        public TaxiRideTSExtractor() {
            super(Time.seconds(MAX_EVENT_DELAY));
        }

        @Override
        public long extractTimestamp(TaxiRide ride) {
            if (ride.isStart) {
              //  return ride.startTime.getMillis();
                return System.currentTimeMillis();
            }
            else {
                //return ride.endTime.getMillis();
                return System.currentTimeMillis();
            }
        }
    }






}

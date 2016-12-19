package de.tuberlin.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import de.tuberlin.io.Conf;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.hadoop.shaded.org.jboss.netty.handler.codec.string.StringDecoder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Properties;

/**
 * Created by patrick on 17.12.16.
 */
public class FlinkWindowFromKafka {


    public FlinkWindowFromKafka(Conf conf) throws Exception {


        final String LOCAL_ZOOKEEPER_HOST = conf.getLocalZookeeperHost();
        final String LOCAL_KAFKA_BROKER = conf.getLocalKafkaBroker();
        final String GROUP_ID = conf.getGroupId();
        final String TOPIC_NAME = conf.getTopicName();
        //private static final String GROUP_ID = "test";

        final int servingSpeedFactor = conf.getServingSpeedFactor(); // events of 10 minutes are served in 1 second
        final int countNumber = conf.getCountSize();         //size of elements in each window
        final int windowTime = conf.getWindowSize();          //measured in seconds
        final int slidingTime = conf.getWindowSlideSize();          //measured in seconds
        final int windowType = conf.getWindowType();
        final String id= new BigInteger(130,new SecureRandom()).toString(32);
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // env.getConfig().setAutoWatermarkInterval(1000);

        // configure the Kafka consumer
        Properties kafkaProps = new Properties();
        //kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("group.id", id);

        kafkaProps.setProperty("max.partition.fetch.bytes","2000");
        // always read the Kafka topic from the start
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("enable.auto.commit", "false");




        // create a Kafka consumer
        FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer010<>(
                TOPIC_NAME,
                new SimpleStringSchema(),
                kafkaProps);
        // assign a timestamp extractor to the consumer
        //    consumer.assignTimestampsAndWatermarks(new org.apache.flink.quickstart.FromKafka.TaxiRideTSExtractor());

        // create a TaxiRide data stream
        DataStream<String> rides = env.addSource(consumer);


        // find average number of passengers per minute starting a taxi ride
        DataStream<Tuple3<Double, String, Long>> averagePassengers = rides

                //filter out those events that are not starting

               // .filter(x -> x.isStart)

                //just keep important variables
                //.map(x->new Tuple3<Integer,Integer,Long>(1,Integer.valueOf(de.tuberlin.source.TaxiRide.fromString(x).passengerCnt),System.currentTimeMillis()))

                .map(new Aggregations.MapPassenger())
                //grouping all values
                .keyBy(0)
                //alternative no keyby , but timeWindowAll
                // sliding window
                .timeWindow(Time.milliseconds(windowTime), Time.milliseconds(slidingTime))
                //.timeWindowAll(Time.milliseconds(windowTime), Time.milliseconds(slidingTime))

                //sums the 1s and the passengers for the whole window
                .reduce(new Aggregations.SumAllValues())
                //.reduce( (x,y)->new Tuple3<Integer, Integer, Long>(x.f0+y.f0,x.f1+y.f1,x.f2<y.f2?y.f2:x.f2))
                .map(new Aggregations.MapToMean());
               // .map(x->new Tuple3<Double, String, Long>(new Double(x.f1*1000/x.f0)/1000.0,String.valueOf(x.f0),System.currentTimeMillis()-x.f2));


        // print result on stdout
        averagePassengers.print();

        //averagePassengers.writeAsCsv("src/main/resources/results/tumbling/"+String.valueOf(System.currentTimeMillis())+"/");

        // execute the transformation pipeline
        env.execute("Windowed Aggregation from Kafka with Flink");
    }



}

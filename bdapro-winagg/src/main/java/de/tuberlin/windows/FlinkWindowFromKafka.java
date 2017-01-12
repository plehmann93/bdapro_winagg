package de.tuberlin.windows;

import de.tuberlin.io.Conf;
import de.tuberlin.serialization.FlinkSimpleStringTsSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerRecord;

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
        final String TOPIC_NAME = "flink-"+conf.getTopicName();
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

        //enable latency tracking
        //env.getConfig().setLatencyTrackingInterval(2000);
        // env.getConfig().setAutoWatermarkInterval(1000);

        // configure the Kafka consumer
        Properties kafkaProps = new Properties();
        //kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        if(conf.getNewOffset()==1){  kafkaProps.setProperty("group.id", id);}else{
            kafkaProps.setProperty("group.id", conf.getGroupId());
        }


        // always read the Kafka topic from the start
        kafkaProps.setProperty("auto.offset.reset", "latest");
        kafkaProps.setProperty("enable.auto.commit", "true");




        // create a Kafka consumer
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(
                TOPIC_NAME,
                new FlinkSimpleStringTsSchema(),
                kafkaProps);

        DataStream<String> rides = env.addSource(consumer);

        // find average number of passengers per minute starting a taxi ride
       DataStream<Tuple4<Double, Long, Long,Long>> averagePassengers = rides



                .map(new Aggregations.MapPassenger())
                //grouping all values

                .keyBy(0)
                //alternative no keyby , but timeWindowAll

                // sliding window
                .timeWindow(Time.milliseconds(windowTime), Time.milliseconds(slidingTime))



                //sums the 1s and the passengers for the whole window
                .reduce(new Aggregations.SumAllValues())
                //.reduce(new Aggregations.SumAllValues(),new Aggregations.TSExtractor())

                .map(new Aggregations.MapToMean())

                ;


        String filePath=conf.getOutputPath()+"flink/"+windowTime+"/"+slidingTime+"/"+conf.getWorkload()+"/"+String.valueOf(System.currentTimeMillis())+".csv";
    if(conf.getWriteOutput()==0){
        // print result on stdout
        averagePassengers.print();
    }else if(conf.getWriteOutput()==1){
        averagePassengers.map(new Aggregations.MapOutput()).writeAsText(filePath);
    }else if(conf.getWriteOutput()==2){
        // print result on stdout
        averagePassengers.print();
        averagePassengers.map(new Aggregations.MapOutput()).writeAsText(filePath);
    }






     //   averagePassengers.writeAsCsv(filePath);

        // execute the transformation pipeline
        env.execute("Windowed Aggregation from Kafka with Flink");
    }



}

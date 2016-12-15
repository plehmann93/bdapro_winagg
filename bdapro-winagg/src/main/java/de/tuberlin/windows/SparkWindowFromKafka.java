package de.tuberlin.windows;

import de.tuberlin.io.Conf;
import de.tuberlin.source.TaxiRide;
import kafka.serializer.StringDecoder;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.SystemClock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by patrick on 15.12.16.
 */
public class SparkWindowFromKafka {

    public SparkWindowFromKafka(Conf conf) throws Exception{

        //spark.streaming.kafka.maxRatePerPartition : Define messages per second to retrive from kafka
        final String LOCAL_ZOOKEEPER_HOST = conf.getLocalZookeeperHost();
        final String APPLICATION_NAME="Spark Window";
        final String LOCAL_KAFKA_BROKER = conf.getLocalKafkaBroker();
        final String GROUP_ID = conf.getGroupId();
        final String TOPIC_NAME=conf.getTopicName();
        final String MASTER=conf.getMaster();


        final int batchsize = conf.getBatchsize();         //size of elements in each window
        final int windowTime = conf.getWindowSize();          //measured in seconds
        final int slidingTime = conf.getWindowSlideSize();          //measured in seconds
        final int partiotions = 1;
        final int multiplication_factor=1;

        Map<String,Integer> topicMap = new HashMap<>();
        topicMap.put("winagg",partiotions);

        SparkConf sparkConf = new SparkConf().setAppName(APPLICATION_NAME).setMaster(MASTER);

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(batchsize*multiplication_factor));

        Set<String> topics = Collections.singleton(TOPIC_NAME);

        Map<String, String>kafkaParams=new HashMap<>();
        kafkaParams.put("metadata.broker.list",LOCAL_KAFKA_BROKER);
        kafkaParams.put("auto.offset.reset","smallest");

        JavaPairInputDStream<String,String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );
        messages
                .map(x -> x._2)
                .window(new Duration(windowTime*multiplication_factor),new Duration(slidingTime*multiplication_factor))
                .map(x->TaxiRide.fromString(x)).print();
               /* .map(new Function<TaxiRide, Tuple3<Integer,Integer,Long>>() {
                    @Override
                    public Tuple3<Integer,Integer,Long> call(TaxiRide taxiRide) {
                        Long time= System.currentTimeMillis();
                        return new Tuple3<Integer,Integer,Long>(1,Integer.valueOf(taxiRide.passengerCnt),time);
                    }
                });
                */
               // .foreachRDD( (rdd,time)-> (rdd,time) );


        jssc.start();

        jssc.awaitTermination();

    }

}

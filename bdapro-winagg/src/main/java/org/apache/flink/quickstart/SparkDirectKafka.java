package org.apache.flink.quickstart;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;
import scala.Tuple2;

import java.util.*;

/**
 * Created by patrick on 15.12.16.
 */
public class SparkDirectKafka {

    public static void main(String[] args) throws Exception{

        //spark.streaming.kafka.maxRatePerPartition : Define messages per second to retrive from kafka

        //String path="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\hadoop\\winutils\\";
        //System.setProperty("hadoop.home.dir", path);
        // Example
        // winutils.exe is copied to C:\winutil\bin\
        // System.setProperty("hadoop.home.dir", "C:\\winutil\\");
        //String logFile = "C:\\sample_log.log";
        String pathToTest="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\test.txt";

        Map<String,Integer> topicMap = new HashMap<>();
        topicMap.put("winagg",1);

        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000));

        //Set<String>topicSet = new HashSet<>(Arrays.asList("winagg"));
        Set<String> topics = Collections.singleton("winagg");
        Map<String, String>kafkaParams=new HashMap<>();
        kafkaParams.put("metadata.broker.list","localhost:9092");
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

       // messages.foreachRDD(rdd -> {
        //    System.out.println("-- New RDD with "+rdd.partitions().size()+" partition and "+rdd.count()+" records");
         //   rdd.foreach(record -> System.out.println(record._2));
        //});
/*w
    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
        @Override
        public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
            return stringStringTuple2._2;
        }
    });


        messages.count().cache().print();
*/
        messages.count().print();
        messages.map(x -> x._2).print();
        jssc.start();

        jssc.awaitTermination();

    }

}

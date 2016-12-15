package org.apache.flink.quickstart;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by patrick on 15.12.16.
 */
public class SparkKafkaExample {


    public static void main(String[] args) throws Exception{


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

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));

        JavaPairReceiverInputDStream<String,String> messages= KafkaUtils.createStream(ssc,"localhost:2181","winagg",topicMap);

        messages.print();

        ssc.start();

        ssc.awaitTermination();

    }

}

package de.tuberlin.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import de.tuberlin.io.Conf;
import de.tuberlin.io.TaxiClass;
import de.tuberlin.source.TaxiRide;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.SystemClock;

import java.io.Serializable;
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
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(batchsize*multiplication_factor));

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
                //.map(x -> x._2)
                .map(x -> TaxiRide.fromString(x._2))
                .map(x->new Tuple3<Integer,Integer,Long>(1,Integer.valueOf(x.passengerCnt),System.currentTimeMillis()))
                .window(new Duration(windowTime*multiplication_factor),new Duration(slidingTime*multiplication_factor))
                .reduce( (x,y)-> new Tuple3<Integer, Integer, Long>(x.f0+y.f0,x.f1+y.f1,x.f2<y.f2?y.f2:x.f2) )
                .map(x->new Tuple3<Double, Integer, Long>(new Double(x.f1*1000/x.f0)/1000.0,x.f0,System.currentTimeMillis()-x.f2))
                .print();
                /*.map(x->new Tuple3<Integer,Integer,Long>(1,TaxiObject.getPassengerCnt(x._2),System.currentTimeMillis()))
                .print();
                .reduce(new Function2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>>() {
                    @Override
                    public Tuple3<Integer, Integer, Long> call(Tuple3<Integer, Integer, Long> t1, Tuple3<Integer, Integer, Long> t2)  throws Exception {
                        Long ts=t2.f2;
                        if(ts<t1.f2){
                            ts=t1.f2;
                        }
                        return new Tuple3<Integer, Integer, Long>(t1.f0+t2.f0,t1.f1+t2.f1,ts);
                    }
                })
                .print();
                /*

                .map(x-> new Tuple3<Double,Integer,Long>(new Double(x.f1*1000/x.f0)/1000.0,x.f1,x.f2))
                .print();
                */
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

    public static class TaxiObject implements Serializable{

        public static int getPassengerCnt(String line){
            String[] values=line.split(",");

            int passengerCnt=Integer.valueOf(values[8]);
            return passengerCnt;
        }
    }


}

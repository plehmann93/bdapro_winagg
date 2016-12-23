package de.tuberlin.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import de.tuberlin.io.Conf;
import de.tuberlin.io.TaxiClass;
import de.tuberlin.source.TaxiRide;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.Consumer;
import scala.Tuple2;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.util.SystemClock;
import scala.Tuple4;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by patrick on 15.12.16.
 */
public class SparkWindowFromKafka implements Serializable{

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
        final int partitions = 1;
        final int multiplication_factor=1;
        final String id= new BigInteger(130,new SecureRandom()).toString(32);

        Map<String,Integer> topicMap = new HashMap<>();
        topicMap.put("winagg",partitions);

        SparkConf sparkConf = new SparkConf()
                .setAppName(APPLICATION_NAME)
               .set("spark.streaming.kafka.maxRatePerPartition",String.valueOf(conf.getWorkload()))
                .set("spark.streaming.backpressure.enabled","true")
                .set("spark.streaming.backpressure.initialRate","1000")
                .setMaster(MASTER);


       JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(batchsize*multiplication_factor));

       // Set<String> topics = Collections.singleton(TOPIC_NAME);
        Collection<String> topics=Arrays.asList(TOPIC_NAME,"win","winagg");
        //Collection<TopicPartition> topics=Arrays.asList(new TopicPartition[]{new TopicPartition(TOPIC_NAME,1)});

       // Map<String, String>kafkaParams=new HashMap<>();
        Map<String,Object>kafkaParams=new HashMap<>();
       // kafkaParams.put("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        //kafkaParams.put("metadata.broker.list",LOCAL_KAFKA_BROKER);
        kafkaParams.put("bootstrap.servers",LOCAL_KAFKA_BROKER);
        kafkaParams.put("auto.offset.reset","earliest");
        kafkaParams.put("enable.auto.commit","false");
        if(conf.getNewOffset()==1){ kafkaParams.put("group.id", id);}else{
            kafkaParams.put("group.id", conf.getGroupId());
        }
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);




        final JavaInputDStream<ConsumerRecord<String,String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferBrokers(),
               // ConsumerStrategies.Assign(topics,kafkaParams)
                ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams)
        );



        Function<Tuple4<Double, Integer, Long,Long>,Integer> printFunction = new Function<Tuple4<Double, Integer, Long, Long>, Integer>() {
            @Override
            public Integer call(Tuple4<Double, Integer, Long, Long> in) throws Exception {
                System.out.println(in);
                return 0;
            }
        };

        SimpleDateFormat sdf=new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

       // messages.map(x->x.)

       JavaDStream<Tuple2<String,Time>> messagesTs= messages.transform(new Function2<JavaRDD<ConsumerRecord<String, String>>, Time, JavaRDD<Tuple2<String,Time>> >() {
            @Override
            public JavaRDD<Tuple2<String,Time>> call(JavaRDD<ConsumerRecord<String, String>> record, Time time) throws Exception {
                                return record.map( (x->new Tuple2<String, Time>(x.value(),time)));
            }
        });

/*
*/

        JavaDStream<Tuple4<Double, Integer, Long,Long>> averagePassengers=messagesTs
               // .map(x -> TaxiRide.fromString(x._2))
                .map(x->new Tuple3<Integer,Long,Long>(1,Long.valueOf(TaxiRide.fromString(x._1).passengerCnt),x._2.milliseconds()))
                //.map(x->new Tuple3<Integer,Long,Long>(1,Long.valueOf(TaxiRide.fromString(x.value()).passengerCnt),System.currentTimeMillis()))


                .reduceByWindow( (x,y)-> new Tuple3<Integer, Long, Long>(x.f0+y.f0,x.f1+y.f1,x.f2<y.f2?y.f2:x.f2)
                        , new Duration(windowTime*multiplication_factor),new Duration(slidingTime*multiplication_factor))

              //  .window(new Duration(windowTime*multiplication_factor),new Duration(slidingTime*multiplication_factor))
              //  .reduce( (x,y)-> new Tuple3<Integer, Long, Long>(x.f0+y.f0,x.f1+y.f1,x.f2<y.f2?y.f2:x.f2) )

                .map(x->new Tuple4<Double, Integer, Long,Long>(new Double(x.f1*1000/x.f0)/1000.0,x.f0,System.currentTimeMillis()-x.f2,x.f2));

        String path="src/main/resources/results/spark/";
        String fileName=windowTime+"/"+slidingTime+"/"+"file";//String.valueOf(System.currentTimeMillis());
        String suffix="";
      averagePassengers.print();
        if(conf.getWriteOutput()==0){
            // print result on stdout
            averagePassengers.print();
        }else if(conf.getWriteOutput()==1){
            averagePassengers.dstream().saveAsTextFiles(path+fileName,suffix);
        }else if(conf.getWriteOutput()==2){
            averagePassengers.print();
            averagePassengers.dstream().saveAsTextFiles(path+fileName,suffix);
        }


        jssc.start();

       // jssc.awaitTermination();
        jssc.awaitTerminationOrTimeout(conf.getTimeout());
        jssc.stop();
    }



}

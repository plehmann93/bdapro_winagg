package de.tuberlin.kafka;

import de.tuberlin.io.Conf;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by patrick on 21.12.16.
 */
public class KafkaProducer extends Thread{
Conf conf;
    public KafkaProducer(Conf conf){
        this.conf=conf;
    }
public void run(){

    writeToKafkaDelayed(conf);


    }

    public void writeToKafkaDelayed(Conf conf) {

        final String TOPIC_NAME = conf.getTopicName();
        final String FILEPATH = conf.getFilepath();

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", "0");
        kafkaProps.put("batch.size", "16384");
        kafkaProps.put("linger.ms", "1");
        kafkaProps.put("buffer.memory", "3354432");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        int maxRate=conf.getWorkload();
        final int numberLoops=conf.getNumberRecords()/maxRate;
        int sleepMillis=(1000/maxRate-1);
        if(sleepMillis<0){sleepMillis=0;}
        int sleepNanos=(1000000000/maxRate-50000)%1000000;
        if(sleepNanos<0){sleepNanos=0;}
        final int SLEEPMILLIS=sleepMillis;
        final int SLEEPNANOS=1000;
        System.out.println("jjjj  "+SLEEPMILLIS+" : "+SLEEPNANOS);

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(kafkaProps);

        try {
            //this.sleep(1000/maxRate-1);
            this.sleep(5000);
        }catch (Exception e){
            e.printStackTrace();
        }

        try {
            for (int i = 0; i < numberLoops; i++) {
           //     Files.lines(Paths.get(FILEPATH)).forEach(x->{
                Long start = System.currentTimeMillis();
                Files.lines(Paths.get(FILEPATH)).limit(maxRate).forEach(x->{
                producer.send(new ProducerRecord<String, String>(TOPIC_NAME,"1",x));
                Long end=System.currentTimeMillis();
                Long sleepTime=999-(end-start);
                //System.out.println(end+" "+start+" "+(end-start)+" "+sleepTime);
                if (sleepTime<0){sleepTime=0L;}
                            try {
                                this.sleep(sleepTime);

                            }catch (InterruptedException e){
                                e.printStackTrace();
                            }
                        }
                );
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("ffffffffffffffffffffinisheeeeeeeeeeeeeeeeeeeeeeeeeeeed");
        System.out.println("ffffffffffffffffffffinisheeeeeeeeeeeeeeeeeeeeeeeeeeeed");
        System.out.println("ffffffffffffffffffffinisheeeeeeeeeeeeeeeeeeeeeeeeeeeed");
    }



}

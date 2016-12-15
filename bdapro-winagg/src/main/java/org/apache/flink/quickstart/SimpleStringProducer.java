package org.apache.flink.quickstart;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Properties;

/**
 * Created by patrick on 15.12.16.
 */
public class SimpleStringProducer {

    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      //  props.put("value.serializer", "org.apache.kafka.common.serialization.Serializer");
        KafkaProducer<String, File> producer = new KafkaProducer<>(props);
     /*   for (int i = 0; i < 1000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("mytesttopic", "value-" + i);
            producer.send(record);
            System.out.println("message "+i+" send");
            try {
                Thread.sleep(250);
            }catch(Exception e){
                e.printStackTrace();
            }

        }
*/

     File file=new File("/home/patrick/Dokumente/nyc200000");

        ProducerRecord<String,File> record=new ProducerRecord<String, File>("mytesttopic",file);
     producer.send(record);
        producer.close();
    }

}

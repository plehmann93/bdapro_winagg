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
public class KafkaProducer {

    public void writeToKafka(Conf conf) {

        final String TOPIC_NAME = conf.getTopicName();
        final String filePath = "src/main/resources/nyc10000";
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", "0");
        kafkaProps.put("batch.size", "16384");
        kafkaProps.put("linger.ms", "1");
        kafkaProps.put("buffer.memory", "3354432");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(kafkaProps);
        try {
            Files.lines(Paths.get(filePath)).forEach(x->
                    producer.send(new ProducerRecord<String, String>(TOPIC_NAME,"1",x))
            );
        } catch (Exception e) {
           e.printStackTrace();
        }

    }
}

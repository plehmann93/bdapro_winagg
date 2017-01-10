package kafkaProducer;

import io.Conf;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import io.Conf;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

public class KafkaProducer {


        Conf conf;
        public KafkaProducer(Conf conf){

            this.conf=conf;
            writeToKafkaLoop(conf);
        }



        public void writeToKafkaLoop(Conf conf) {
            Random random = new Random();
            String systemname="spark";
            if (conf.getFlink() == 1) {
                systemname="flink";
            }else if(conf.getFlink() == 4){
                systemname="flink";
            }
            final String TOPIC_NAME = systemname+"-"+conf.getTopicName();
            final String FILEPATH = conf.getFilepath();
            List<String> lines=new ArrayList<>() ;
            try {
                lines=Files.readAllLines(Paths.get(FILEPATH));
            }catch(IOException e) {
                e.printStackTrace();
            }

            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", conf.getLocalKafkaBroker());
            kafkaProps.put("acks", "all");
            kafkaProps.put("retries", "0");
            kafkaProps.put("batch.size", "16384");
            kafkaProps.put("linger.ms", "1");
            kafkaProps.put("buffer.memory", "3354432");
            kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            System.out.println("Starting writing to "+TOPIC_NAME);
            int fileSize=lines.size();
            int maxRate=conf.getWorkload();
            final int numberLoops=conf.getNumberRecords()/maxRate;
            final int numberFiles=maxRate/fileSize;
            final int rest=maxRate%fileSize;




            //Files.lines(Paths.get(FILEPATH)).

            Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(kafkaProps);



            Long startWriting=System.currentTimeMillis();
            Long difference=Long.valueOf(conf.getTimeout());
            while(System.currentTimeMillis()-startWriting<difference){

                Long start = System.currentTimeMillis();
                for (int j = 0; j < numberFiles; j++) {
                    lines.stream().forEach(x->{
                        producer.send(new ProducerRecord<String, String>(TOPIC_NAME,String.valueOf(random.nextInt(9) + 1),x));
                    });
                }
                lines.stream().limit(rest).forEach(x-> {
                    producer.send(new ProducerRecord<String, String>(TOPIC_NAME,String.valueOf(random.nextInt(9) + 1), x));
                });
                Long end=System.currentTimeMillis();
                Long sleepTime=999-(end-start);
                //System.out.println(end+" "+start+" "+(end-start)+" "+sleepTime);
                if (sleepTime<0){sleepTime=0L;}

                try {
                    Thread.sleep(sleepTime);
                }catch(InterruptedException e) {
                    System.out.println("Process finished");
                }

            }



        }


    }



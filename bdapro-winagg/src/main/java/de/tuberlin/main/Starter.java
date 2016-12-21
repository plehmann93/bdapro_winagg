package de.tuberlin.main;

import de.tuberlin.io.Conf;
import de.tuberlin.kafka.KafkaProducer;
import de.tuberlin.windows.*;

/**
 * Created by Lehmann on 01.12.2016.
 */
public class Starter {

    public static void main(String[] args) throws Exception {
        Conf conf = new Conf();
       // new KafkaProducer().writeToKafka(conf); //for writing into kafka
        if (conf.getFlink() == 1) {
            new FlinkWindowFromKafka(conf);

        }else{
            new SparkWindowFromKafka(conf);
        }
    }
}



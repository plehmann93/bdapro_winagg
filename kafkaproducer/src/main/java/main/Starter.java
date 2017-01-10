package main;

import io.Conf;
import kafkaProducer.KafkaProducer;

/**
 * Created by Lehmann on 10.01.2017.
 */
public class Starter {

    public static void main(String[] args) throws Exception {
    System.out.println("Begin with writing "+args[0]+" "+args[1]+" "+args[2]);
        Conf conf;
        if (args.length == 0) {
            conf = new Conf();
        } else {
            conf = new Conf(args[0]);
        }
        if (args.length == 2) {
            conf.setFilepath(args[1]);
        }
        if (args.length == 3) {
            conf.setWorkload(Integer.valueOf(args[2]));
        }
        //start kafka producer in parallel
        if (conf.getKafkaProducer() == 1) {
            new KafkaProducer(conf);
        }


    }
}

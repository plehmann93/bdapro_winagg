package de.tuberlin.main;

import de.tuberlin.io.Conf;
import de.tuberlin.windows.*;

/**
 * Created by Lehmann on 01.12.2016.
 */
public class Starter {

    public static void main(String[] args) throws Exception {
        Conf conf = new Conf();
        if (conf.getFlink() == 1) {
            new FlinkWindowFromKafka(conf);
            System.out.println("4");
            /*
            if (conf.getFromKafka() == 0) {
                switch (conf.getWindowType()) {
                    case 1:
                        new Tumbling(conf);
                        break;
                    case 2:
                        new Sliding(conf);
                        break;
                    case 3:
                        new Count(conf);
                        break;
                }
            }else {
                switch (conf.getWindowType()) {
                    case 1:
                        new FlinkTumblingWindowFromKafka(conf);
                        break;
                    case 2:
                        new FlinkSlidingWindowFromKafka(conf);
                        break;
                    case 3:
                        new FlinkCountWindowFromKafka(conf);
                        break;
                }

            }
            */
        }else{
            new SparkWindowFromKafka(conf);
        }
    }
}



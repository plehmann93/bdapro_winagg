package de.tuberlin.io;

import org.ini4j.Wini;

import java.io.File;
import java.io.IOException;

/**
 * Created by Lehmann on 01.12.2016.
 */
public class Conf {

 int windowType;  // tumbling|sliding|count
 int windowSize;  // measured in seconds
 int windowSlideSize;
 int countSize;
 int servingSpeedFactor;// 600 = events of 10 minutes are served in 1 second
 int maxEventDelay; // 60 =  events are out of order by max 60 seconds
 String localZookeeperHost;
 String localKafkaBroker;
 String groupId;
 String topicName;
 int fromKafka;
 int flink;
    public int getWindowType() {
        return windowType;
    }


    public int getWindowSize() {
        return windowSize;
    }


    public int getCountSize() {return countSize;}

    public int getWindowSlideSize() {
        return windowSlideSize;
    }

    public int getServingSpeedFactor() {return servingSpeedFactor;}

    public int getMaxEventDelay() {return maxEventDelay;}

    public String getLocalZookeeperHost() {return localZookeeperHost; }

    public String getLocalKafkaBroker() { return localKafkaBroker;    }

    public String getGroupId() {        return groupId;    }

    public String getTopicName() {        return topicName;    }

    public int getFromKafka() {       return fromKafka;    }

    public int getFlink() {        return flink;    }

    public Conf(){

    try {
        Wini ini = new Wini(new File("src/main/resources/config.ini"));

        windowType = ini.get("window", "window_type", int.class);
        windowSize = ini.get("window", "window_size", int.class);
        windowSlideSize = ini.get("window", "window_slide_size", int.class);
        countSize = ini.get("window", "count_number", int.class);
        servingSpeedFactor = ini.get("kafka", "serving_speed_factor", int.class);
        maxEventDelay = ini.get("kafka", "max_event_delay", int.class);
        localZookeeperHost= ini.get("kafka", "local_zookeeper_host");
        localKafkaBroker= ini.get("kafka", "local_kafka_broker");
        groupId= ini.get("kafka", "group_id");
        topicName= ini.get("kafka", "topic_name");
        flink= ini.get("system", "flink", int.class);
        fromKafka= ini.get("system", "from_kafka", int.class);

    }catch (IOException e){
        e.printStackTrace();
    }

    }
}

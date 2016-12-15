package org.apache.flink.quickstart;

/**
 * Created by Lehmann on 13.12.2016.
 */
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.Iterator;

public class Sparkexample {

    public static void main(String[] args) throws Exception {
        String path="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\hadoop\\winutils\\";
        System.setProperty("hadoop.home.dir", path);
        String pathToTaxi="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\nyc200000";
        String pathToTaxi2="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\nycTaxiRides";
        String pathToTest="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\test.txtd";
        //String pathToTest="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\test.txt";
        //SparkConf conf = new SparkConf().setAppName("sparktest").setMaster("local[*]");
        SparkConf conf = new SparkConf().setAppName("sparktest").setMaster("local");
       // JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));
       // JavaRDD<String> distFile = sc.textFile(pathToTaxi);
        //ssc.textFileStream(pathToTaxi).print();
        //ssc.
       // JavaRDD<Integer> da=distFile.map(s -> (s.length()));

        JavaDStream lines=ssc.textFileStream(pathToTest);

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override public Iterator<String> call(String x) {
                        return Arrays.asList(x.split(" ")).iterator();
                    }
                });
        //num.foreachRDD(System.out.println());
        words.print();
        ssc.start();
        ssc.awaitTermination();
        //num.foreachRDD();
        //System.out.println("jooooooo "+num.toString());


/*        avaPairInputDStream<Text, Text> myDStream =
                jssc.fileStream(path, Text.class, Text.class, customInputFormat.class, new Function<Path, Boolean>() {
                    @Override
                    public Boolean call(Path v1) throws Exception {
                        return Boolean.TRUE;
                    }
                }, false);

*/
    }
}

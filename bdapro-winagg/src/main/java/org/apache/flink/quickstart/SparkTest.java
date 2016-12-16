package org.apache.flink.quickstart;

/**
 * Created by Lehmann on 13.12.2016.
 */
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SparkTest {

    public static void main(String[] args) {


     //   String path="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\hadoop\\winutils\\";
      //  System.setProperty("hadoop.home.dir", path);
        // Example
        // winutils.exe is copied to C:\winutil\bin\
        // System.setProperty("hadoop.home.dir", "C:\\winutil\\");
        //String logFile = "C:\\sample_log.log";
        String pathToTest="C:\\Users\\Lehmann\\Documents\\Studium\\Informatik\\BigData\\test.txt";
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD logData = sc.textFile(pathToTest).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        System.out.println("Lines with a: " + numAs);

    }

}

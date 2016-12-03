package de.tuberlin.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Created by Lehmann on 01.12.2016.
 */
public class Aggregations {

    /**
     Maps Taxiride so just id of ride and passengercount stays
     */
    public static class MapToPassenger implements MapFunction<TaxiRide, Tuple3<Integer, Integer,Long>> {

        @Override
        public Tuple3<Integer, Integer,Long> map(TaxiRide taxiRide) throws Exception {
            return new Tuple3<Integer,Integer,Long>(1, Integer.valueOf(taxiRide.passengerCnt),System.currentTimeMillis() );

        }
    }


    /**
     Maps Taxiride so just id of ride and passengercount stays
     */
    public static class MapToMean implements MapFunction< Tuple3<Integer, Integer,Long> , Tuple3<Double, String,Long>> {


        public Tuple3<Double, String,Long> map( Tuple3<Integer, Integer,Long> t) throws Exception {
            Long millis=System.currentTimeMillis();
            String timeStamp = new Date(millis).toString();
            return new Tuple3<Double,String,Long>(Math.round(t.f1/new Double(t.f0)*1000)/1000.0,timeStamp, millis-t.f2);

        }
    }

    public static class SumAllValues implements ReduceFunction<Tuple3<Integer, Integer,Long>> {
        @Override
        public Tuple3<Integer, Integer,Long> reduce(Tuple3<Integer, Integer,Long> value1, Tuple3<Integer, Integer,Long> value2) throws Exception {
            return new Tuple3<Integer, Integer,Long>(value1.f0+value2.f0, value1.f1+value2.f1,value1.f2);
        }
    }


    /**
     * Returns the average number of passengers in a specific time window
     */
    public static class RideCounter implements WindowFunction<
            Tuple2<Long, Integer>, // input type
            Tuple4<Double,Integer,String,String>, // output type
            Tuple, // key type
            TimeWindow> // window type
    {

        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Tuple2<Long, Integer>> values,
                Collector<Tuple4<Double,Integer,String,String>> out) throws Exception {

//            Long cellId = ((Tuple2<Long, Integer>)key).f0;
            //           Integer passenger = ((Tuple2<Long, Integer>)key).f1;
            long windowTime = window.getStart();
            String dateString=new Date(windowTime).toString();
            Double cnt = 0.0;
            Double sum = 0.0;

            for(Tuple2<Long, Integer> v : values) {
                cnt += 1;
                sum += v.f1;
            }

            Date timeStamp= new Date(System.currentTimeMillis());
            out.collect(new Tuple4<>(Double.valueOf( Math.round(sum/cnt*1000.0)/1000.0),cnt.intValue(),dateString,timeStamp.toString() ));
            //out.collect(new Tuple1<>( Double.valueOf( Math.round(cnt*100.0)/100.0)));
        }
    }

}


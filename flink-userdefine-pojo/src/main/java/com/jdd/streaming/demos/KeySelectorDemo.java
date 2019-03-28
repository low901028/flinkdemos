package com.jdd.streaming.demos;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;


/**
 * @Auther: dalan
 * @Date: 19-3-25 13:25
 * @Description:
 */
public class KeySelectorDemo {
    // main
    public static void main(String[] args) throws Exception {
         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         DataStream<String> ints = env.fromElements("a","b","c","aa","bb","cc","aaa","bbb","ccc");
         //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

         DataStream<Tuple2<String,Long>> mins = ints
             .map(new MapFunction<String, Tuple2<String, Long>>() {
                 @Override
                 public Tuple2<String, Long> map(String data) throws Exception {
                     return new Tuple2<String, Long>(data,1L);
                 }
             })
             .keyBy(0)
             .timeWindow(Time.of(5, TimeUnit.SECONDS))
             .sum(1);
//             .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
//                 @Override
//                 public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
//                    Iterator<Tuple2<String, Long>> itor = iterable.iterator();
//                    Long sum = 0L;
//                    while (itor.hasNext()){
//                        Tuple2<String, Long> t = itor.next();
//                        sum += t.f1;
//                    }
//
//                    System.out.println(
//                            "\n the key is " + tuple.toString()
//                            + "\n (key,value) sets is " + itor.toString()
//                            + "\n the sum is " + sum
//                    );
//
//                    collector.collect(new Tuple2<String, Long>(tuple.toString(), sum));
//                 }
//             });



         mins.print();

         env.execute("the key aggregations demo");

        Thread.sleep(10000);
    }
}

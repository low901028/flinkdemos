package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Auther: dalan
 * @Date: 19-3-25 11:33
 * @Description:
 */
public class FoldDemo {
    /** logger */
    //private static final Logger LOGGER = LoggerFactory.getLogger(FoldDemo.class);
     // main
     public static void main(String[] args) throws Exception {
          final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          //env.getConfig().disableSysoutLogging();

          DataStream<String> source = env.fromElements("hello","world","hello","world","hello","world");

          source.print();

         KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
              @Override
              public Tuple2<String, Integer> map(String s) throws Exception {
                  return new Tuple2<String, Integer>(s, 1);
              }
          })
          .keyBy(0);

         DataStream<Tuple2<String, Integer>> aggregationDatas =
                 keyedStream
                 .sum(1);

         aggregationDatas.print();



        // fold的使用: 该操作不建议使用
//         DataStream<String> foldDatas =  keyedStream.fold("test", new FoldFunction<Tuple2<String, Integer>, String>() {
//              @Override
//              public String fold(String s, Tuple2<String, Integer> o) throws Exception {
//                  return s + "=" + o;
//              }
//          });
//
//          foldDatas.print();

          env.execute("the fold method is used.");
     }
}

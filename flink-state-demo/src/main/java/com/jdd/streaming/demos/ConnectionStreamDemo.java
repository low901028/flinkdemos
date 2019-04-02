package com.jdd.streaming.demos;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @Auther: dalan
 * @Date: 19-3-28 19:23
 * @Description:
 */
public class ConnectionStreamDemo {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionStreamDemo.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStream<String> control = env.fromElements("DROP", "IGNORE").keyBy(x->x);
        //DataStream<String> streamOfWords = env.fromElements("data", "DROP", "artisans", "IGNORE").keyBy(x->x);

        //
        // 保证both streams 要么都分组 要么都不分组
        // 1.分组的情况 对应的key值必须是相同的类型
        // 2.分组的情况 要求相对开放
        DataStream<Tuple2<String, Integer>> control = env.fromCollection(Arrays.asList(
            new Tuple2<String, Integer>("hello", 1),
            new Tuple2<String, Integer>("world", 1),
            new Tuple2<String, Integer>("good", 1),
            new Tuple2<String, Integer>("come", 1),
            new Tuple2<String, Integer>("here", 1),
            new Tuple2<String, Integer>("go", 1)
        ));//.keyBy(x->x.f0);


        DataStream<Tuple3<String, Integer,Integer>> streamOfWords = env.fromCollection(Arrays.asList(
            new Tuple3<String, Integer,Integer>("hello", 3, 1),
            new Tuple3<String, Integer,Integer>("world", 3, 1),
            new Tuple3<String, Integer,Integer>("good", 3, 2)
        ));//.keyBy(x->x.f0);

//        control.connect(streamOfWords)
//            .flatMap(new RichCoFlatMapFunction<String, String, Object>() {
//                @Override public void flatMap1(String s, Collector<Object> collector) throws Exception {
//                    System.out.println("===flatMap1===");
//                    blocked.update(Boolean.TRUE);
//                }
//
//                @Override public void flatMap2(String s, Collector<Object> out) throws Exception {
//                    System.out.println("===flatMap2===");
//                    if(blocked.value() == null){
//                        out.collect(s);
//                    }
//                }
//
//                private transient ValueState<Boolean> blocked;
//
//                @Override public void open(Configuration config){
//                    blocked = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("", Boolean.class));
//                }
//            })
        control.connect(streamOfWords)
            .flatMap(new RichCoFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, Object>() {
                @Override public void flatMap1(Tuple2<String, Integer> in, Collector<Object> out) throws Exception {
                    //Tuple3<String, Integer, Integer> result = Tuple3.of(in.f0, in.f1, 0);
                    System.out.println("flatMap1\t" + in.toString());
                    out.collect(in);
                }

                @Override public void flatMap2(Tuple3<String, Integer, Integer> in, Collector<Object> out) throws Exception {
                    System.out.println("flatMap2\t" + in.toString());
                    out.collect(in);
                }
            })
            .print();

        env.execute();
    }


}

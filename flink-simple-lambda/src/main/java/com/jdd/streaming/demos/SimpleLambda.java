package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @Auther: dalan
 * @Date: 19-3-21 09:54
 * @Description:
 */
public class SimpleLambda {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleLambda.class);
    // main
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromCollection(Arrays.asList(1,2,3,4,5,6))
            .map(i -> Tuple2.of(i, i))
            .returns(Types.TUPLE(Types.INT, Types.INT)) // 当转换操作存在泛型时 可以通过指定TypeInformation来处理类型擦除带来的问题
            .print();

        /**
        .flatMap(new FlatMapFunction<Integer, Integer>() {  // 在flink使用带有泛型的类型存在类型擦除 需要明确指定对应的泛型具体类型
            @Override
            public void flatMap(Integer in, Collector<Integer> out) throws Exception {
                out.collect(in + in);
            }
        })
        .map(i -> i * i)  // lambda的使用
        .print();
        */

        env.execute("a simple lambda demo");
    }
}

package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * @Auther: dalan
 * @Date: 19-3-28 17:00
 * @Description:
 */
public class SimpleState {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleState.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final String[] items = {"hello","world","go","come","here"};

        DataStream<Tuple2<String,Integer>> input = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private volatile boolean isRunning = true;
            private Random rand = new Random(9527);

            @Override public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (isRunning){
                    Tuple2<String,Integer> item = new Tuple2<>(items[rand.nextInt(items.length)],rand.nextInt(10)+1);
                    ctx.collect(item);
                    // 模拟暂停
                    Thread.sleep(100);
                }
            }

            @Override public void cancel() {
                isRunning = false;
            }
        });

        DataStream<Tuple2<String,Integer>> smoothed =
            input.keyBy(0)
                 .map(new Smoother());

        smoothed.print();

        env.execute("s simple state demo");
    }

    public static  class Smoother extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>{
        private transient ValueState<Tuple2<String, Integer>> averageState;

        @Override public void open(Configuration config) throws Exception{ // 由parallel数决定 需要启动执行几次open
            System.out.println("execute only one");
            ValueStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ValueStateDescriptor<Tuple2<String, Integer>>(
                    "test state",
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                );
            averageState = getRuntimeContext().getState(descriptor);
        }

        @Override public Tuple2<String, Integer> map(Tuple2<String, Integer> item) throws Exception {
            Tuple2<String, Integer> average = averageState.value();

            if(average == null) {
                average = new Tuple2<>("test", 0);
            }

            //System.out.println("=======" + average.toString());

            //更新
            average.f1 += item.f1;
            averageState.update(average);

            if(average.f1 >= 2){
                averageState.clear(); // 执行state清除
                return new Tuple2<>(item.f0, (item.f1 + average.f1));
            }
            return new Tuple2<>("test",0);
        }
    }
}

package com.jdd.streaming.demos;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @Auther: dalan
 * @Date: 19-3-21 16:43
 * @Description:
 */
public class UserDefineWaterMark {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefineWaterMark.class);

    // main
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        Long sourceLatenessMillis = (Long)params.getLong("source-lateness-millis");
        Long maxLaggedTimeMillis = params.getLong("window-lagged-millis");
        Long windowSizeMillis = params.getLong("window-size-millis");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 基于event
        DataStream<String> streams = env.addSource(new StringLineEventSource(sourceLatenessMillis));

        //解析输入的数据
        DataStream<String> inputMap = ((DataStreamSource<String>) streams)
        .setParallelism(1)
        .assignTimestampsAndWatermarks( // 指派时间戳，并生成WaterMark
                new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(maxLaggedTimeMillis)){
                    @Override
                    public long extractTimestamp(String s) {
                        return NumberUtils.toLong(s.split("\t")[0]);
                    }
                })
        .setParallelism(2)
        .map(new MapFunction<String, Tuple2<Tuple2<String,String>, Long>>() {
            @Override
            public Tuple2<Tuple2<String,String>, Long> map(String value) throws Exception {
                String[] arr = value.split("\t");
                String channel = arr[1];
                return new Tuple2<Tuple2<String,String>, Long>(Tuple2.of(channel, arr[3]), 1L);
            }
        })
        .setParallelism(2)
        //.keyBy(0,1)
        .keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSizeMillis)))
        .process(new ProcessWindowFunction<Tuple2<Tuple2<String, String>, Long>, Object, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<Tuple2<String, String>, Long>> iterable, Collector<Object> collector) throws Exception {
                // <key, Iterable<>>

                long count = 0;
                Tuple2<String,String> tuple2 = null;
                for (Tuple2<Tuple2<String, String>, Long> in : iterable){
                    tuple2 = in.f0;
                    count++;
                }

                LOGGER.info("window===" + tuple.toString());
                collector.collect(new Tuple6<String, Long, Long,String,String,Long>(tuple2.getField(0).toString(),context.window().getStart(), context.window().getEnd(),tuple.getField(0).toString(),tuple.getField(1).toString(),count));
            }
        })
        .setParallelism(4)
        .map(t -> {
            Tuple6<String, Long, Long,String,String,Long> tt = (Tuple6<String, Long, Long,String,String,Long>)t;
            Long windowStart = tt.f1;
            Long windowEnd = tt.f2;
            String channel = tt.f3;
            String behaviorType = tt.f4;
            Long count = tt.f5;
            return StringUtils.join(Arrays.asList(windowStart, windowEnd, channel, behaviorType, count) ,"\t");
        })
        .setParallelism(3);

        inputMap.addSink((new FlinkKafkaProducer011<String>("localhost:9092,localhost:9092","windowed-result-topic",new SimpleStringSchema())))
                .setParallelism(3);

        //inputMap.print();

        env.execute("EventTime and WaterMark Demo");
    }
}

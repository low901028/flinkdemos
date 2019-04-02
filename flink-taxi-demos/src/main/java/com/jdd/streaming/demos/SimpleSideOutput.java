package com.jdd.streaming.demos;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.calcite.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @Auther: dalan
 * @Date: 19-4-2 11:35
 * @Description:
 */
public class SimpleSideOutput {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSideOutput.class);
    public static void main(String[] args) throws Exception {
        final OutputTag<SimpleWaterMark.Event> REJECTEDWORDSTAG = new OutputTag<SimpleWaterMark.Event>("rejected_words_tag"){};

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String[] datas = {"hello","world","good","yes","ok","here"};
        String[] ops = {"-","+"};

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        DataStream<SimpleWaterMark.Event> strs = env.addSource(new SourceFunction<SimpleWaterMark.Event>() {
            private Random rand = new Random(9527);
            private volatile boolean isRunning = true;
            private volatile Long nums =0L;


            @Override public void run(SourceContext<SimpleWaterMark.Event> out) throws Exception {
                final long cts = System.currentTimeMillis();

                // 模拟延迟数据
                final ScheduledExecutorService exec = new ScheduledThreadPoolExecutor(1);
                exec.scheduleAtFixedRate(new Runnable() {
                    @Override public void run() {
                      SimpleWaterMark.Event e = new SimpleWaterMark.Event(datas[rand.nextInt(datas.length)], ops[rand.nextInt(2)].equals("+")? (cts + rand.nextInt(100)) : (cts - rand.nextInt(100)) );
                        System.out.println(
                            "======single thread event=====" + e + " current_thread_id " + Thread.currentThread().getId());
                        out.collect(e);
                    }}, 3, 4, TimeUnit.SECONDS);

                // 模拟正常数据
                while (isRunning && nums < 500){
                    long ts = System.currentTimeMillis();
                    SimpleWaterMark.Event e = new SimpleWaterMark.Event(datas[rand.nextInt(datas.length)], ops[rand.nextInt(2)].equals("+")? (ts + rand.nextInt(100)) : (ts - rand.nextInt(100)) );
                    System.out.println("======event=====" + e + " current_thread_id " + Thread.currentThread().getId());
                    out.collect(e);

                    nums++;
                    Thread.sleep(rand.nextInt(50)+10);
                }
                exec.shutdownNow();
            }

            @Override public void cancel() {
                isRunning = false;
            }
        });

        SingleOutputStreamOperator<SimpleWaterMark.Event> sides = strs
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SimpleWaterMark.Event>(Time.of(2L, SECONDS)) {
                private volatile Long currentTimestamp = 0L;
                @Override public long extractTimestamp(SimpleWaterMark.Event event) {
                    long ts = event.ts;
                    currentTimestamp = ts > currentTimestamp ? ts : currentTimestamp;
                    return ts;
                }
            })
            .keyBy("name")
//            .process(new KeyedProcessFunction<String, SimpleWaterMark.Event, SimpleWaterMark.Event>() {
//                @Override
//                public void processElement(SimpleWaterMark.Event event, Context ctx, Collector<SimpleWaterMark.Event> out)
//                    throws Exception {
//                    String key = event.name;
//                    if(key.length() >= 5){
//                        ctx.output(REJECTEDWORDSTAG, event);
//                    }else if (key.length() > 0){
//                        out.collect(event);
//                    }
//                }
//            })
            //.timeWindow(Time.of(2, SECONDS))
            .window(TumblingEventTimeWindows.of(Time.seconds(2)))
            .sideOutputLateData(REJECTEDWORDSTAG)
            .apply(new WindowFunction<SimpleWaterMark.Event, SimpleWaterMark.Event, Tuple, TimeWindow>() {
                @Override
                public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SimpleWaterMark.Event> iterable,
                    Collector<SimpleWaterMark.Event> out) throws Exception {
                    Iterator<SimpleWaterMark.Event> iter = iterable.iterator();
                    List<SimpleWaterMark.Event> events = IteratorUtils.toList(iter);
                    Collections.sort(events);
                    for (SimpleWaterMark.Event e: events) {
                        out.collect(e);
                    }

                    System.out.println("the time window " +
                        "\tstart " + timeWindow.getStart()+
                        "\tend " + timeWindow.getEnd() +
                        "\tkey " + tuple.toString() +
                        "\telement_size " + events.size());

                }
            });

        // 记录延迟数据可单独做处理
        DataStream<String> events =
            sides.getSideOutput(REJECTEDWORDSTAG)
                 .map(new MapFunction<SimpleWaterMark.Event, String>() {
                     @Override public String map(SimpleWaterMark.Event event) throws Exception {
                         return "rejected_"+event;
                     }
                 });
        events.print();

        env.execute("a simple sideoutput demo");
    }
}

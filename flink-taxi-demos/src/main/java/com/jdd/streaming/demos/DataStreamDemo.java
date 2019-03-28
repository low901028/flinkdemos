package com.jdd.streaming.demos;

import com.jdd.streaming.demos.entity.TaxiFare;
import com.jdd.streaming.demos.entity.TaxiRide;
import com.jdd.streaming.demos.source.TaxiFareSource;
import com.jdd.streaming.demos.source.TaxiRideSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Auther: dalan
 * @Date: 19-3-25 15:19
 * @Description:
 */
public class DataStreamDemo {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamDemo.class);
    // main
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String taxiRide = params.get("taxi-ride-path","/home/wmm/go_bench/flink_sources/nycTaxiRides.gz");
        String taxiFare = params.get("taxi-fare-path","/home/wmm/go_bench/flink_sources/nycTaxiFares.gz");

         //
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(taxiRide, maxEventDelay, servingSpeedFactor));
        //DataStream<TaxiFare> fares = env.addSource(new TaxiFareSource(taxiFare, maxEventDelay, servingSpeedFactor));
        env.getConfig().setParallelism(1);

        DataStream<String> strs = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random rand = new Random();
                while (true) {
                    ctx.collect(TESTSTR[rand.nextInt(TESTSTR.length)]);
                    Thread.sleep(100);  // 模拟 让数据collect延迟100ms 便于window操作的输出:窗口大小=1s 每隔500ms执行一次
                }
            }

            @Override
            public void cancel() { // 此处不需要处理
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStrs = strs
            .map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String s) throws Exception {
                    return new Tuple2(s, 1);
                }
            })
            .keyBy(0)
            ;

        DataStream<String> result = keyedStrs
                .timeWindow(Time.seconds(1))
                .apply(new WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {
                        Iterator<Tuple2<String, Integer>> iter = iterable.iterator();
                        long sum = 0;
                        while (iter.hasNext()){
                            Tuple2<String, Integer> t = iter.next();
                            sum += t.f1;
                        }

                        LOGGER.info("key = " + tuple.toString()
                                + "\tval = " + sum + "\n"
                                + "window start = " + timeWindow.getStart() + "\t window end " + timeWindow.getEnd() + "\n");

                        collector.collect("key = " + tuple.toString() + "\tval = " + sum);
                    }
                }).map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        return s;
                    }
                });
        //
        result.print();

//        DataStream<Tuple2<String, Integer>> flatStrs = strs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                collector.collect(new Tuple2<String, Integer>(s, 1));
//            }
//        }).setParallelism(3);
//        DataStream<Tuple2<String, Integer>> reStrs = flatStrs.rebalance();
//        DataStream mapStrs = reStrs.map(new MapFunction<Tuple2<String, Integer>, String>() {
//            @Override
//            public String map(Tuple2<String, Integer> s) throws Exception {
//                return new String("key= " + s.f0 + "\tval= " + s.f1);
//            }
//        });
//        mapStrs.print();

        env.execute("the datastream api demos");
    }

    // 测试模拟数据
    private  static String[] TESTSTR = {"hello","world","good","yes","no","it","no","come"};
}

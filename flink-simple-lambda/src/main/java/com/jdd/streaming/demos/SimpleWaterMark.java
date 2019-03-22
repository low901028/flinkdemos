package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Auther: dalan
 * @Date: 19-3-22 09:40
 * @Description:
 */
public class SimpleWaterMark {
    private static  Random rand = new Random(7569);

    // main
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        Long delayedTimestamp = params.getLong("delayedTimestamp",10000);

        //获取运行环境
       final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用eventtime，默认是使用processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度为1,默认并行度是当前机器的cpu数量
        env.setParallelism(1);

        List<String> datas = SimpleWaterMark.generatStrings(delayedTimestamp);
        //读取测试数据
        DataStream<String> text = env.fromCollection(datas);


        //解析输入的数据: <key, timestamp>格式
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<String, Long>(arr[0], Long.parseLong(arr[1]));
            }
        });

        //抽取timestamp和生成watermark
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 10000L;// 最大允许的乱序时间是10s

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {  // 生成watermark: 考虑了延迟最大容忍时间
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            //定义如何提取timestamp: 使用event默认自带的eventtime
            // 当watermark的时间>=window_end_time则会触发对应的窗口计算
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                long id = Thread.currentThread().getId();

                System.out.println("<key,timestamp> :"+element.f0+",事件时间[eventTime]:[ "+sdf.format(element.f1)+" ], 最大序列时间[currentMaxTimestamp]:[ "+
                        sdf.format(currentMaxTimestamp)+" ], 水印时间[waterMark]:[ "+sdf.format(getCurrentWatermark().getTimestamp())+" ]");

                return timestamp;
            }
        });

        DataStream<String> window = waterMarkStream.keyBy(0) // 基于<key,timestamp>来进行分组
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))//按照消息的EventTime分配窗口，和调用TimeWindow效果一样
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    // 通过WindowFunction函数对window内的数据进行排序，保证数据的顺序
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<Long>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();

                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();

                            System.out.println(next.toString());

                            arrarList.add(next.f1);
                        }
                        Collections.sort(arrarList);

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = "\n 键值 : "+ key
                                + "\n              触发窗内数据 : " + arrarList
                                + "\n              触发窗内数据个数 : " + arrarList.size()
                                + "\n              触发窗起始数据： " + sdf.format(arrarList.get(0))
                                + "\n              触发窗最后（可能是延时）数据：" + sdf.format(arrarList.get(arrarList.size() - 1))
                                + "\n              实际窗起始和结束时间： " + sdf.format(window.getStart()) + "《----》" + sdf.format(window.getEnd())
                                + " \n \n ";

                        out.collect(result);
                    }
                });
        //测试-把结果打印到控制台即可
        window.print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("eventtime-watermark");
    }

    // 简单的数据生成
    public static List<String> generatStrings(long latenessMills) throws InterruptedException {
        List<String> channelSet = Arrays.asList("a", "b", "c", "d");

        //Long delayedTimestamp = Instant.ofEpochMilli(System.currentTimeMillis())
        //        .minusMillis(latenessMills)
        //        .toEpochMilli();


        //System.out.println(delayedTimestamp);
        List<String> datas = new ArrayList<String>();
        for(int i=0; i<10; i++){
            String channel = channelSet.get(rand.nextInt(channelSet.size()));
            Long delayedTimestamp = System.currentTimeMillis();

            String data = channel + "," + delayedTimestamp;

            datas.add(data);

            TimeUnit.MILLISECONDS.sleep(5L);
        }
        //System.out.println(datas);
        return datas;
    }
}

package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 测试: nc -ln 9000
 *      0001,1538359882000
 *      0002,1538359886000
 *      0003,1538359892000
 *      0004,1538359893000
 *      0005,1538359894000
 *      0006,1538359896000
 *      0007,1538359897000
 *      0008,1538359897000
 *      0009,1538359872000 此条信息比较触发sideoutput的存储 已超出Window的有效时间
 * @Auther: dalan
 * @Date: 19-4-2 15:36
 * @Description:
 */
public class SocketSideOutput {
    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        int port = 9000;
        //获取运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用eventtime，默认是使用processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度为1,默认并行度是当前机器的cpu数量
        env.setParallelism(4);
        //连接socket获取输入的数据
        DataStream<String> text = env.socketTextStream("localhost", port, "\n");
        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        //抽取timestamp和生成watermark
        // 设定水印current_watermark = max(event.timestamp) 同时设置最大可忍受延迟时间=1s;
        // 通过使用current_watermark - 最大可忍受event延迟时间,将对应的watermark代表的窗口结束时间前移来接受延迟的event
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 1000L;  // 最大可忍受延迟时间1s
            // 最大允许的乱序时间是10s
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            /**
             * 定义生成watermark的逻辑 * 默认100ms被调用一次
             */
            @Nullable @Override public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            //定义如何提取timestamp
            @Override public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
//                System.out.println("key:" + element.f0 + ",eventtime:[" + element.f1 + "|" + sdf.format(element.f1) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp)
//                    + "],watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp())
//                    + "]");
                return timestamp;
            }
        });

        //保存被丢弃的数据
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};
        //注意，由于getSideOutput方法是SingleOutputStreamOperator子类中的特有方法，所以这里的类型，不能使用它的父类dataStream。
        SingleOutputStreamOperator<String> window =
            waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.SECONDS)))
                //按照消息的EventTime分配窗口，和调用TimeWindow效果一样
                // .allowedLateness(Time.seconds(2)) //允许数据迟到2秒
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                /**
                 * 对window内的数据进行排序，保证数据的顺序
                 *
                 * @param tuple
                 * @param window
                 * @param input
                 * @param out
                 * @throws Exception
                 */
                @Override public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                    String key = tuple.toString();
                    List<Long> arrarList = new ArrayList<Long>();
                    Iterator<Tuple2<String, Long>> it = input.iterator();
                    while (it.hasNext()) {
                        Tuple2<String, Long> next = it.next();
                        arrarList.add(next.f1);
                    }
                    Collections.sort(arrarList);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    String result = key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0)) + "," + sdf
                        .format(arrarList.get(arrarList.size() - 1)) + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                    out.collect(result);

                    //System.out.println(result);
                }
            });

        //把迟到的数据暂时打印到控制台，实际中可以保存到其他存储介质中
        // 本处延迟的event已经超过指定Window的[start,end)有效范围,并且在已忍受可延迟最大周期的基础上出现延迟的信息
        DataStream<Tuple2<String, Long>> sideOutput = window.getSideOutput(outputTag);
        sideOutput.print();
        //测试-把结果打印到控制台即可 window.print();
        // 注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("eventtime-watermark");
    }
}
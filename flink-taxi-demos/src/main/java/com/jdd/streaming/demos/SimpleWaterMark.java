package com.jdd.streaming.demos;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @Auther: dalan
 * @Date: 19-4-1 17:49
 * @Description:
 */
public class SimpleWaterMark {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        String[] datas = {"hello","world","good","yes","ok","here"};
        String[] ops = {"-","+"};

        DataStream<Event> strs = env.addSource(new SourceFunction<Event>() {
            private Random rand = new Random(9527);
            private volatile boolean isRunning = true;

            @Override public void run(SourceContext<Event> out) throws Exception {
                while (isRunning){
                    long ts = System.currentTimeMillis();
                    Event e = new Event(datas[rand.nextInt(datas.length)], ops[rand.nextInt(2)].equals("+")? (ts + rand.nextInt(100)) : (ts - rand.nextInt(100)) );
                    System.out.println("======event=====" + e);
                    out.collect(e);
                    Thread.sleep(rand.nextInt(50)+10);
                }
            }

            @Override public void cancel() {
                isRunning = false;
            }
        });

        DataStream<Event> events = strs.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.of(5, SECONDS)) {
            @Override public long extractTimestamp(Event e) {
                System.out.println("===watermark generation===");
                return e.ts;
            }
        }).keyBy("name")
          .timeWindow(Time.of(5, SECONDS))
          .apply(new WindowFunction<Event, Event, Tuple, TimeWindow>() {
              @Override public void apply(Tuple tuple,   // 由于timewindow需要和KeyedStream结合使用 故而此处提供的是key
                  TimeWindow timeWindow,                 // 此处指定了TimeWindow窗口的[start, end)
                  Iterable<Event> iterable,              // 包括[start, end)范围TimeWindow内所有的数据: 在window中的数据需要结合实际业务进行设定防止Window数据过大
                  Collector<Event> out) throws Exception { // 将TimeWindow中的内容投递到下一个transform
                  String key = tuple.toString();
                  Iterator<Event> iter = iterable.iterator();
                  List<Event> events = IteratorUtils.toList(iter);
                  Collections.sort(events);

                  for (Event e : events){
                      out.collect(e);
                  }

                  System.out.println(timeWindow +
                      "\tkey=="+ key +
                      "\telement_size==" + events.size() +
                      "\twindow_start==" + timeWindow.getStart() +
                      "\twindow_end=="+timeWindow.getEnd());
              }
          });

        events.print();

        env.execute("a simple watermark demo");
    }

    public  static  class Event implements Comparable<Object>{
        public String name;
        public long ts;

        public Event(){}
        public Event(String name, Long ts){
            this.name = name;
            this.ts = ts;
        }

        @Override public String toString(){
            return "the event name = " + this.name
                + "\tts=" + this.ts;
        }

        @Override public int hashCode(){
            return (int)this.ts;
        }

        @Override public int compareTo(Object other){
            if (other == null){
                return -1;
            }
            if(other instanceof Event){
                Event e = (Event) other;
                if(this.ts < e.ts){
                    return -1;
                }else if(this.ts > e.ts){
                    return 1;
                }else{
                    return 0;
                }
            }

            return 0;
        }
    }
}

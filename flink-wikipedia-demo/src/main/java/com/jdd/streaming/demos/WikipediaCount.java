package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @Auther: dalan
 * @Date: 19-3-20 14:17
 * @Description:
 */
public class WikipediaCount {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(WikipediaCount.class);

    // main
    public static void main(String[] args) throws Exception {
        // 创建environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // wiki source
        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());
        // 处理数据
        DataStream<Tuple2<String, Long>> datas = edits
          .map(new MapFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
              @Override
              public Tuple2<String, Long> map(WikipediaEditEvent event) throws Exception {
                  return new Tuple2<>(event.getUser(), (long)(event.getByteDiff()));
              }
          })
          .keyBy(0)
          .timeWindow(Time.seconds(5))
          .sum(1);

          datas.map(new MapFunction<Tuple2<String, Long>, String>() {
              @Override
              public String map(Tuple2<String, Long> t) throws Exception {
                  return t.toString();
              }
          })
          .addSink(new FlinkKafkaProducer011<String>("localhost:9092,localhost:9092","wiki-result",new SimpleStringSchema()));
          // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wiki-result --from-beginning 查看写入的message

// 默认实现自定义的key selector: 选择user为key;
// 定义一个滚动窗口时效5s
//  使用fold完成统计
//
//        .keyBy(new KeySelector<WikipediaEditEvent, Object>() {  // 实现key selector
//            @Override
//            public Object getKey(WikipediaEditEvent event) throws Exception {
//                return event.getUser();
//            }
//        })
//        .timeWindow(Time.seconds(5)) // 定义一个滚动窗口:5s周期的
//        .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
//            @Override
//            public Tuple2<String, Long>  fold(Tuple2<String, Long>  t, WikipediaEditEvent o) throws Exception {
//                t.f0 = o.getUser();
//                t.f1 += (o.getByteDiff());
//                return t;
//            }
//        });

        datas.print();

        env.execute("Wikipedia diff bytes count");
    }
}

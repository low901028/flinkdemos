package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        final String hostName;
        final int port;

        final ParameterTool params = ParameterTool.fromArgs(args);
        hostName = params.has("hostname") ? params.get("hostname") : "localhost";
        port = params.getInt("port");

        // 使用flink的步骤
        // 1、构建一个ExecutionEnvironment
        // 2、创建Source，对应的是DataStream即视为数据源
        // 3、执行算子链 得到自己想要的结果；同时需要定义结果类(map---> operators...--->reduce)
        // 4、对生产的结果sink处理
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream(hostName, port, "\n");

        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                for (String word : s.split("\\s")){
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        })
        .keyBy("word")
        .timeWindow(Time.seconds(5))
        .reduce(new ReduceFunction<WordWithCount>() {
            public WordWithCount reduce(WordWithCount t1, WordWithCount t2) throws Exception {
                return new WordWithCount(t1.word, t1.count + t2.count);
            }
        });

        windowCounts.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }



    public static class WordWithCount{ // 定义
        public String word;
        public long count;

        public WordWithCount(){}
        public WordWithCount(String word, long count){
            this.count = count;
            this.word = word;
        }

        public String toString(){
            return word + " : " + count;
        }
    }
}



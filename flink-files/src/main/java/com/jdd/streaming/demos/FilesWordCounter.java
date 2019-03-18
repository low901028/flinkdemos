package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class FilesWordCounter {
    public static void main(String[] args) throws Exception {
        final String filePath;

        final ParameterTool params = ParameterTool.fromArgs(args);
        filePath = params.has("path") ? params.get("path") : "";

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile(filePath);

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        // normalize and split the line
                        String[] tokens = s.toLowerCase().split("\\W+");

                        // emit the pairs
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                collector.collect(new Tuple2<String, Integer>(token, 1));
                            }
                        }
                    }
                })
                // group by the tuple field "0" and sum up tuple field "1"
                .groupBy(0)
                .sum(1);
        counts.print();
    }

    public static class WordCounter{
        public String word;
        public long count;

        public WordCounter(){}
        public WordCounter(String word, long count){
            this.count = count;
            this.word = word;
        }

        public String toString(){
            return word + " : " + count;
        }
    }
}

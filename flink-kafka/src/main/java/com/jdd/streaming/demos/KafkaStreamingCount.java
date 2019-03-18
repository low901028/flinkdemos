package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;

public class KafkaStreamingCount {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (params.getNumberOfParameters() < 5) {
            System.out.println("Missing Paramters!");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.getConfig().disableSysoutLogging();
//        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
//        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
//        env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<KafkaEvent> input = env
                .addSource(new FlinkKafkaConsumer011<KafkaEvent>(
                        params.getRequired("input-topic"),
                        new KafkaEventSchema(),
                        params.getProperties()
                ))
                //.assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
                .flatMap(new FlatMapFunction<KafkaEvent, KafkaEvent>() {
                    //@Override
                    public void flatMap(KafkaEvent kafkaEvent, Collector<KafkaEvent> collector) throws Exception {
                        if (null != kafkaEvent && !StringUtils.isNullOrWhitespaceOnly(kafkaEvent.word)) {
                            for (String word : kafkaEvent.word.split("\\s")){
                                collector.collect(new KafkaEvent(word, 1, System.currentTimeMillis()));
                            }
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<KafkaEvent>() {
                    public KafkaEvent reduce(KafkaEvent t1, KafkaEvent t2) throws Exception {
                       return new KafkaEvent(t1.word, t1.frequency + t2.frequency);
                    }
                 })
                ;
        input.print();

        env.execute("Kafka Word count");
    }

    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent> {

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        //@Override
        public long extractTimestamp(KafkaEvent event, long previousElementTimestamp) {
            // the inputs are assumed to be of format (message,timestamp)
            this.currentTimestamp = event.getTimestamp();
            return event.getTimestamp();
        }

        @Nullable
        //@Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }


    public static class KafkaEvent{
        private String word;
        private int frequency;
        private long timestamp;

        public KafkaEvent() {}

        public KafkaEvent(String word, int frequency) {
            this.word = word;
            this.frequency = frequency;
        }


        public KafkaEvent(String word, int frequency, long timestamp) {
            this.word = word;
            this.frequency = frequency;
            this.timestamp = timestamp;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public static KafkaEvent fromString(String eventStr) {
            //String[] split = eventStr.split(",");
            //return new KafkaEvent(split[0], Integer.valueOf(split[1]), Long.valueOf(split[2]));
            return new KafkaEvent(eventStr,1);
        }

        @Override
        public String toString() {
            return word + "," + frequency + "," + timestamp;
        }
    }
}

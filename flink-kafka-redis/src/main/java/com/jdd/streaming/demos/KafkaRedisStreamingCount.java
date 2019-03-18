package com.jdd.streaming.demos;

import com.jdd.streaming.demos.sink.RedisCommand;
import com.jdd.streaming.demos.sink.RedisConfig;
import com.jdd.streaming.demos.sink.RedisPushCommand;
import com.jdd.streaming.demos.sink.RedisSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class KafkaRedisStreamingCount {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRedisStreamingCount.class);

    public  static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        // --topic

        if (params.getNumberOfParameters() < 5) {
            System.out.println("Missed parametes!\n" +
                    "Usage: Kafka --input-topic <topic> \n" +
                    "--bootstrap.servers <kafka brokers> \n" +
                    "--zookeeper.connect <zk quorum> --group.id <some id> \n" +
                    "--output-redis <redis>\n");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 进行全局配置
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<RedisCommand> datas =
                env.addSource(new FlinkKafkaConsumer011<String>(
                    params.getRequired("input-topic"),
                    new SimpleStringSchema(),
                    params.getProperties())
        ).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        // normalize and split the line
                        String[] tokens = value.toLowerCase().split("\\W+");

                        // emit the pairs
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                out.collect(new Tuple2<String, Integer>(token, 1));
                            }
                        }
                    }
                })
        .keyBy(0)
        .sum(1)//.setParallelism(2)
        .keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<String, Integer>(t1.f0, (t1.f1 + t2.f1));
                    }
                })
        .map(new MapFunction<Tuple2<String, Integer>, RedisCommand>() {
            public RedisCommand map(Tuple2<String, Integer> in) {
                LOGGER.info(in.f0, in.f1);

                return new RedisPushCommand(in.f0, new String[]{in.f1.toString()},1000);
            }
        });

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.setHost(params.get("output-redis"));
        redisConfig.setPort(6379);
        redisConfig.setPassword(null);
        RedisSink redisSink = new RedisSink(redisConfig);

        if(null != datas){
            datas.print();
            datas.addSink(redisSink);
        }

        //datas.addSink(new K)
        env.execute("Word count by kafka and redis");
    }
}

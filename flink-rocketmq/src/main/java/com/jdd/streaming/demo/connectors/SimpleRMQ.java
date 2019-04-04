package com.jdd.streaming.demo.connectors;

import com.jdd.streaming.demo.connectors.common.RocketMQConfig;
import com.jdd.streaming.demo.connectors.selector.SimpleTopicSelector;
import com.jdd.streaming.demo.connectors.serialization.KeyValueDeserializationSchema;
import com.jdd.streaming.demo.connectors.serialization.KeyValueSerializationSchema;
import com.jdd.streaming.demo.connectors.serialization.SimpleKeyValueDeserializationSchema;
import com.jdd.streaming.demo.connectors.serialization.SimpleKeyValueSerializationSchema;
import com.jdd.streaming.demo.connectors.sink.RocketMQSink;
import com.jdd.streaming.demo.connectors.source.RocketMQSource;
import io.netty.util.internal.ObjectUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @Auther: dalan
 * @Date: 19-4-3 17:15
 * @Description:
 */
public class SimpleRMQ {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleRMQ.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        final Properties props = new Properties();
        props.setProperty(RocketMQConfig.NAME_SERVER_ADDR,"127.0.0.1:9876");
        props.setProperty(RocketMQConfig.CONSUMER_TOPIC,"TopicTest");
        props.setProperty(RocketMQConfig.CONSUMER_GROUP,"*");
        props.setProperty(RocketMQConfig.CONSUMER_PULL_POOL_SIZE, "2");
        props.setProperty(RocketMQConfig.CONSUMER_TAG,"TagA");
        //props.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO,"latest");

        DataStream<Map> messages = env.addSource(new RocketMQSource<>(new SimpleKeyValueDeserializationSchema("key","value"), props));
        //messages.print();

        SingleOutputStreamOperator<Tuple2<String, String>> streamMessages = messages.map(new MapFunction<Map, Tuple2<String, String>>() {
            @Override public Tuple2<String, String> map(Map map) throws Exception {
                //System.out.println(" === " + map);

                String key = map.get("key").toString();
                String value = map.get("value").toString();

                return new Tuple2<String, String>(key, value);
            }
        });

        //streamMessages.print();

        streamMessages.addSink(new RocketMQSink(
            new SimpleKeyValueSerializationSchema(),
            new SimpleTopicSelector("TopicTest1","topic","TagB","tag"),
            props));

        env.execute("a simple rocketmq demo");
    }
}

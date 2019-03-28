package com.jdd.streaming.demos;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Random;

/**
 * @Auther: dalan
 * @Date: 19-3-27 10:35
 * @Description:
 */
public class TimeCharacteristicDemo {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeCharacteristicDemo.class);
    // main
    public static void main(String[] args) throws Exception {
         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

         // 指定数据源
         DataStream<User> users = env.addSource(new SourceFunction<User>() {
             private boolean isRunnable = true;
             @Override public void run(SourceContext<User> ctx) throws Exception {
                 Random rand = new Random(7569);
                while(isRunnable){
                    User user  = new User("User_" + rand.nextInt(10),
                                                rand.nextInt(100),
                                                System.currentTimeMillis() + rand.nextInt(100) - 100
                    );
                    ctx.collect(user);
                    Thread.sleep(100);
                }
             }

             @Override public void cancel() {
                isRunnable = false;
             }
         });
        users.print();


        DataStream<User> ages = users
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<User>(Time.minutes(1L)) {  // 在Source后 指定assign timestamp 和 generetic
                @Override public long extractTimestamp(User user) {
                    return user.timestamp;
                }
            })
            .keyBy(user -> user.user) // 基于user字段进行分组
            .timeWindow(Time.minutes(1L))
            .reduce((t1,t2) -> new User(t1.user,t1.age + t2.age, t2.timestamp)); // 针对Window的记录进行reduce操作
            //.map((user)->user);

        ages.print();

        env.execute("a event time window demo");
    }

    public static class User implements Serializable {
        public String user;
        public Integer age;
        public  Long timestamp;

        public User(){}
        public User(String user, Integer age, Long timestamp){
            this.user = user;
            this.age = age;
            this.timestamp = timestamp;
        }

        @Override
        public String toString(){
            return "the user " + this.user
                + "\tage " + this.age
                + "\ttimestamp " + this.timestamp;
        }
    }
}

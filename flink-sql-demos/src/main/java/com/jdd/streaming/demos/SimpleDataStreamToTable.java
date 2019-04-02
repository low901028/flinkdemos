package com.jdd.streaming.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Random;

/**
 * @Auther: dalan
 * @Date: 19-3-29 11:22
 * @Description:
 */
public class SimpleDataStreamToTable {
    public static String[] names = {"Tom", "Bob", "Bill", "Robin", "kalo", "jack"};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<User> users =  env.addSource(new SourceFunction<User>() {
            private volatile boolean isRunning = true;
            private Random rand = new Random(9527);

            @Override public void run(SourceContext<User> out) throws Exception {
                while (isRunning){ //模拟stream数据
                    User user = new User(names[rand.nextInt(names.length)], rand.nextInt(100) + 1, "user content");
                    out.collect(user);
                    Thread.sleep(50);
                }
            }

            @Override public void cancel() {
                isRunning = false;
            }
        });


        // 创建table context
        final StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        // DataStream ---> table
        Table user = tEnv.fromDataStream(users);
        Table result = user.filter("name='Tom'")
            .select("name, age, content");

        tEnv.toAppendStream(result, User.class).print();

        env.execute("a simple datastream to table demo");
    }

    public  static  class User{
        public String name;
        public Integer age;
        public String content;

        public User(){  }
        public User(String name, Integer age, String content){
            this.name = name;
            this.age = age;
            this.content = content;
        }

        @Override public String toString(){
            return "user name=" + this.name
                + "\tage=" + this.age
                + "\tcontent=" + this.content;
        }
    }


}

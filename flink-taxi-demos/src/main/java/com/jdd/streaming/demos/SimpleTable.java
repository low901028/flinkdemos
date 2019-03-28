package com.jdd.streaming.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Locale;

/**
 * @Auther: dalan
 * @Date: 19-3-28 14:51
 * @Description:
 */
public class SimpleTable {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTable.class);

    private static transient DateTimeFormatter timeFormatter =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.CHINA).withZoneUTC();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<SimpleOrder> orders = env.fromCollection(Arrays.asList(
            new SimpleOrder(12345L, new DateTime(), (short)3),
            new SimpleOrder(23456L, new DateTime(), (short)4),
            new SimpleOrder(34567L, new DateTime(), (short)2),
            new SimpleOrder(45678L, new DateTime(), (short)1)
        ));

//        DataStream<SimpleOrder> orders = env.fromCollection(Arrays.asList(
//            new SimpleOrder(12345L, new DateTime(), 3),
//            new SimpleOrder(23456L, new DateTime(), 4),
//            new SimpleOrder(34567L, new DateTime(), 2),
//            new SimpleOrder(45678L, new DateTime(), 1)
//        ));

        Table order = tEnv.fromDataStream(orders);
        Table result = tEnv.sqlQuery("SELECT * FROM " + order);
        tEnv.toAppendStream(result, SimpleOrder.class) // table中对应count字段为是int类型 实际pojo类中需要的是short类型 故而在该处会抛出异常
            .print();


        env.execute();
    }

    // 当定义的POJO类在进行from table to datastream出现类型转换错误:一般是由于table和datastream指定的字段类型不兼容:
    // 通过定义bean并设置对应的getter/setter方法解决
    // 直接使用pojo的public字段 会导致类型不兼容问题
//    public static class SimpleOrder{
//        private long id;
//        private DateTime createTime;
//        private short count;
//        // public int count;
//
//        public SimpleOrder(){}
//        public SimpleOrder(long id, DateTime createTime, short count){
//            this.id = id;
//            this.createTime = createTime;
//            this.count = count;
//        }
//
//        @Override
//        public String toString(){
//            return "id = " + this.id
//                + "\tcreateTime= " + createTime.toString(timeFormatter)
//                + "\tcount=" + this.count;
//        }
//    }

    public static class SimpleOrder{
        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public DateTime getCreateTime() {
            return createTime;
        }

        public void setCreateTime(DateTime createTime) {
            this.createTime = createTime;
        }

        public short getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = (short)count; // 强制类型转换
        }

        private long id;
        private DateTime createTime;
        private short count;
        // public int count;

        public SimpleOrder(){}
        public SimpleOrder(long id, DateTime createTime, short count){
            this.id = id;
            this.createTime = createTime;
            this.count = count;
        }

        @Override
        public String toString(){
            return "id = " + this.id
                + "\tcreateTime= " + createTime.toString(timeFormatter)
                + "\tcount=" + this.count;
        }

    }
}

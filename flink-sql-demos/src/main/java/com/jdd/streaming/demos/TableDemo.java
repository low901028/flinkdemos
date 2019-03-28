package com.jdd.streaming.demos;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @Auther: dalan
 * @Date: 19-3-28 10:10
 * @Description:
 */
public class TableDemo {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDemo.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Order> orders1 = env.fromCollection(Arrays.asList(
            new Order(1L, "beer", 3),
            new Order(1L, "diaper", 4),
            new Order(3L, "rubber", 2)
            )
        );
        DataStream<Order> orders2 = env.fromCollection(Arrays.asList(
            new Order(2L, "pen", 3),
            new Order(2L, "rubber", 3),
            new Order(4L, "beer", 1)
            )
        );
        Table t = tEnv.fromDataStream(orders1,"user, product, amount");
        tEnv.registerDataStream("orders2", orders2, "user, product, amount");
        Table result = tEnv.sqlQuery("SELECT * FROM " + t + " WHERE amount > 2 UNION ALL " +
            " SELECT * FROM orders2 WHERE amount < 2");


        tEnv.toAppendStream(result, Order.class).print();

        env.execute("a simple table demo");

    }

    // 定义POJO类
    public static class Order implements Serializable{
        public Long user;
        public String product;
        public Integer amount;

        public Order() {
        }

        public Order(Long user, String product, Integer amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                "user=" + user +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
        }
    }
}

package com.jdd.streaming.demos;

import com.jdd.streaming.demos.entity.TaxiRide;
import com.jdd.streaming.demos.sink.RedisConfig;
import com.jdd.streaming.demos.source.TaxiRideSource;
import com.jdd.streaming.demos.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Auther: dalan
 * @Date: 19-3-19 11:24
 * @Description:
 */
public class TaxiRideCleansing {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(TaxiRideCleansing.class);
    // main
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input","/home/wmm/go_bench/flink_sources/nycTaxiRides.gz");
        final int parallelism = params.getInt("parallelism",4);

        final int maxEventDelay = 60;
        final int servingSpeedFactor = 600;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(parallelism);

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));
        DataStream<TaxiRide> filterRides = rides
                .filter(new FilterFunction<TaxiRide>() { // 过滤数据
                    @Override
                    public boolean filter(TaxiRide taxiRide) throws Exception { // 剔除行程超出纽约城的
                        return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                                GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
                    }
                })
//                .map(new MapFunction<TaxiRide, TaxiRide>() { // 定义一个map函数
//                    @Override
//                    public TaxiRide map(TaxiRide taxiRide) throws Exception {
//                        return new TaxiRide.EnrichedRide(taxiRide);
//                    }
//                })
                .flatMap(new FlatMapFunction<TaxiRide, TaxiRide>() { // 定义一个flatMap: one-to-one
                    @Override
                    public void flatMap(TaxiRide taxiRide, Collector<TaxiRide> collector) throws Exception {
                        FilterFunction<TaxiRide> valid = new FilterFunction<TaxiRide>() {
                            @Override
                            public boolean filter(TaxiRide taxiRide) throws Exception {
                                return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                                        GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
                            }
                        };
                        if(valid.filter(taxiRide)){
                            collector.collect(taxiRide);
                        }

                    }
                })
                ;

        // 自定义一个sink
        /**
        RedisConfig redisConfig = new RedisConfig();
        redisConfig.setHost(params.get("output-redis","127.0.0.1"));
        redisConfig.setPort(6379);
        redisConfig.setPassword(null);

        filterRides.addSink(new RichSinkFunction<TaxiRide>() {
            private transient JedisPool jedisPool;
            @Override
            public void open(Configuration parameters) throws Exception {
                try {
                    super.open(parameters);

                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxIdle(redisConfig.getMaxIdle());
                    config.setMinIdle(redisConfig.getMinIdle());
                    config.setMaxTotal(redisConfig.getMaxTotal());
                    jedisPool = new JedisPool(config, redisConfig.getHost(), redisConfig.getPort(),
                            redisConfig.getConnectionTimeout(), redisConfig.getPassword(), redisConfig.getDatabase());
                } catch (Exception e) {
                    LOGGER.error("redis sink error {}", e);
                }
            }

            @Override
            public void close() throws Exception {
                try {
                    jedisPool.close();
                } catch (Exception e) {
                    LOGGER.error("redis sink error {}", e);
                }
            }

            @Override
            public void invoke(TaxiRide val, Context context) throws Exception {
                Jedis jedis = null;
                try {
                    jedis = jedisPool.getResource();
                    jedis.set("taxi:ride:nyc:" + val.rideId,val.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (null != jedis){
                        if (jedis != null) {
                            try {
                                jedis.close();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        });
         */
        filterRides.print();

        env.execute("Taxi Ride Cleansing Not In NYC");
    }
}

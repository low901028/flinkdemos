package com.jdd.streaming.demos;

import com.jdd.streaming.demos.entity.TaxiFare;
import com.jdd.streaming.demos.entity.TaxiRide;
import com.jdd.streaming.demos.sink.RedisCommand;
import com.jdd.streaming.demos.sink.RedisConfig;
import com.jdd.streaming.demos.sink.RedisPushCommand;
import com.jdd.streaming.demos.sink.RedisSink;
import com.jdd.streaming.demos.source.TaxiRideSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import sun.util.resources.ga.LocaleNames_ga;

/**
 * @Auther: dalan
 * @Date: 19-3-18 15:09
 * @Description:
 */
public class TaxiRideCount {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(TaxiRideCount.class);
    // main
    public static void main(String[] args) throws Exception {
        // 读取配置参数: 文件路径/最大延迟时间/
        final ParameterTool params = ParameterTool.fromArgs(args);
        String path = params.get("file-path","/home/wmm/go_bench/flink_sources/nycTaxiRides.gz");
        int maxDeply = params.getInt("max-delay",60);
        int servingSpeed = params.getInt("serving-speed",600);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().disableSysoutLogging();

        // 指定TaxiRide
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(path, maxDeply, servingSpeed));

        DataStream<Tuple2<Long,Long>> tuples = rides.map(new MapFunction<TaxiRide, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(TaxiRide ride) throws Exception {
                return new Tuple2<Long, Long>(ride.driverId, 1L); // 基于行程中的司机id划分数据 并进行统计
            }
        });

        KeyedStream<Tuple2<Long, Long>, Tuple> keyByDriverId = tuples.keyBy(0); // 基于司机id进行数据划分
        DataStream<Tuple2<Long, Long>> rideCounts = keyByDriverId.sum(1); // 累计每个司机的里程数

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.setHost(params.get("output-redis","127.0.0.1"));
        redisConfig.setPort(6379);
        redisConfig.setPassword(null);
        RedisSink redisSink = new RedisSink(redisConfig);

        rideCounts.map(new MapFunction<Tuple2<Long, Long>, RedisCommand>() { // 落地redis
            @Override
            public RedisCommand map(Tuple2<Long, Long> in) throws Exception {
                return new RedisPushCommand("taxi:ride:" + in.f0, Long.toString(in.f1));
                //return new RedisPushCommand("taxi:ride:" + in.f0, new String[]{Long.toString(in.f1)});
            }
        }).addSink(redisSink);

        /** // 直接使用匿名类实现redis sink
        rideCounts.addSink(new RichSinkFunction<Tuple2<Long, Long>>() {  // 定义sink
            private transient JedisPool jedisPool;
            @Override
            public void open(Configuration parameters) throws Exception { // 新建redis pool
                try {
                    super.open(parameters);
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setHost(params.get("output-redis","127.0.0.1"));
                    config.setPort(6379);
                    config.setPassword(null);
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
            public void close() throws Exception { // 关闭redis链接
                try {
                    jedisPool.close();
                } catch (Exception e) {
                    LOGGER.error("redis sink error {}", e);
                }
            }

            @Override
            public void invoke(Tuple2<Long, Long> val, Context context) throws Exception { // 执行将内容落地redis
                Jedis jedis = null;
                try {
                    jedis = jedisPool.getResource();
                    jedis.set(val.f0.toString(),val.f1.toString());
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
        //rideCounts.print();

        JobExecutionResult result = env.execute("Ride Count By DriverID");
     }
}

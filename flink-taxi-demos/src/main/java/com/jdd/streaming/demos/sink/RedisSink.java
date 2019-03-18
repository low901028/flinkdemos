package com.jdd.streaming.demos.sink;

/**
 * @Auther: dalan
 * @Date: 19-3-15 14:38
 * @Description:
 */
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/***
 * redis sink
 *
 * support any operation
 * support set expire
 */
public class RedisSink extends RichSinkFunction<RedisCommand> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

    private final RedisConfig redisConfig;

    private transient JedisPool jedisPool;

    public RedisSink(RedisConfig redisConfig) {
        this.redisConfig = Preconditions.checkNotNull(redisConfig, "Redis client config should not be null");
    }


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
            LOG.error("redis sink error {}", e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            jedisPool.close();
        } catch (Exception e) {
            LOG.error("redis sink error {}", e);
        }
    }


    private Jedis getJedis() {
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    public void closeResource(Jedis jedis) {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void invoke(RedisCommand command, Context context) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            command.execute(jedis);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != jedis)
                closeResource(jedis);
        }
    }

}

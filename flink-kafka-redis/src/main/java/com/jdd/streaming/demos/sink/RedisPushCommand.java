package com.jdd.streaming.demos.sink;

/**
 * @Auther: dalan
 * @Date: 19-3-15 14:41
 * @Description:
 */
import redis.clients.jedis.Jedis;

/**
 * override rpush command
 */
public class RedisPushCommand extends RedisCommand {
    public RedisPushCommand(){super();}

    public RedisPushCommand(String key, Object value) {
        super(key, value);
    }

    public RedisPushCommand(String key, Object value, int expire) {
        super(key, value, expire);
    }

    @Override
    public void invokeByCommand(Jedis jedis) {
        jedis.rpush(getKey(), (String[]) getValue());
    }


}
package com.jdd.streaming.demos.sink;

/**
 * @Auther: dalan
 * @Date: 19-3-15 14:39
 * @Description:
 */
import redis.clients.jedis.Jedis;

import java.io.Serializable;

public abstract class RedisCommand implements Serializable {
    String key;
    Object value;
    int expire;

    public RedisCommand(){}

    public RedisCommand(String key, Object value, int expire) {
        this.key = key;
        this.value = value;
        this.expire = expire;
    }


    public RedisCommand(String key, Object value) {
        this.key = key;
        this.value = value;
        this.expire = -1;
    }

    public void execute(Jedis jedis) {
        invokeByCommand(jedis);
        if (-1 < this.expire) {
            jedis.expire(key, expire);
        }
    }

    public abstract void invokeByCommand(Jedis jedis);

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }
}

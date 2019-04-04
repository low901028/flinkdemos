package com.jdd.streaming.demo.connectors.serialization;

import java.io.Serializable;

/**
 * @Auther: dalan
 * @Date: 19-4-2 19:09
 * @Description:
 */
public interface KeyValueSerializationSchema<T> extends Serializable {
    byte[] serializeKey(T tuple);   // 序列化key
    byte[] serializeValue(T tuple); // 序列化value
}

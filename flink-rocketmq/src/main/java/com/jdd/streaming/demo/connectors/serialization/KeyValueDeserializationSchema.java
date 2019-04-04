package com.jdd.streaming.demo.connectors.serialization;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

/**
 * @Auther: dalan
 * @Date: 19-4-2 19:09
 * @Description:
 */
public interface KeyValueDeserializationSchema<T>  extends ResultTypeQueryable<T>, Serializable {
    T deserializeKeyAndValue(byte[] key, byte[] vlaue); // 反序列化
}

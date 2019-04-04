package com.jdd.streaming.demo.connectors.serialization;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @Auther: dalan
 * @Date: 19-4-2 19:23
 * @Description: rocketmq的内容采用map存储:Map<<Key,Key_value>,<Value_key,value>>
 */
public class SimpleKeyValueSerializationSchema implements KeyValueSerializationSchema<Map> {
    public static final String DEFAULT_KEY_FIELD = "key";
    public static final String DEFAULT_VALUE_FIELD = "value";

    public String keyField;
    public String valueField;

    public SimpleKeyValueSerializationSchema() {
        this(DEFAULT_KEY_FIELD, DEFAULT_VALUE_FIELD);
    }

    /**
     * SimpleKeyValueSerializationSchema Constructor.
     * @param keyField tuple field for selecting the key
     * @param valueField  tuple field for selecting the value
     */
    public SimpleKeyValueSerializationSchema(String keyField, String valueField) {
        this.keyField = keyField;
        this.valueField = valueField;
    }

    @Override
    public byte[] serializeKey(Map tuple) {
        if (tuple == null || keyField == null) {
            return null;
        }
        Object key = tuple.get(keyField);
        return key != null ? key.toString().getBytes(StandardCharsets.UTF_8) : null;
    }

    @Override
    public byte[] serializeValue(Map tuple) {
        if (tuple == null || valueField == null) {
            return null;
        }
        Object value = tuple.get(valueField);
        return value != null ? value.toString().getBytes(StandardCharsets.UTF_8) : null;
    }

}
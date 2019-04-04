package com.jdd.streaming.demo.connectors.selector;

import java.io.Serializable;

/**
 * @Auther: dalan
 * @Date: 19-4-2 19:34
 * @Description:
 */
public interface TopicSelector<T> extends Serializable {

    String getTopic(T tuple);

    String getTag(T tuple);
}

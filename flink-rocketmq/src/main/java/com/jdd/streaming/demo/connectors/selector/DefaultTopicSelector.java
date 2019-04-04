package com.jdd.streaming.demo.connectors.selector;

import java.util.Map;

/**
 * @Auther: dalan
 * @Date: 19-4-2 19:35
 * @Description:
 */
public class DefaultTopicSelector<T> implements TopicSelector<T> {
    private final String topicName;
    private final String tagName;

    public DefaultTopicSelector(final String topicName, final String tagName) {
        this.topicName = topicName;
        this.tagName = tagName;
    }

    public DefaultTopicSelector(final String topicName) {
        this(topicName, "");
    }

    @Override
    public String getTopic(T tuple) {
        return topicName;
    }

    @Override
    public String getTag(T tuple) {
        return tagName;
    }
}

package com.jdd.streaming.demo.connectors.selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Auther: dalan
 * @Date: 19-4-2 19:38
 * @Description:
 */
public class SimpleTopicSelector implements TopicSelector<Map> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTopicSelector.class);

    private final String topicFieldName;
    private final String defaultTopicName;

    private final String tagFieldName;
    private final String defaultTagName;

    /**
     * SimpleTopicSelector Constructor.
     * @param topicFieldName field name used for selecting topic
     * @param defaultTopicName default field name used for selecting topic
     * @param tagFieldName field name used for selecting tag
     * @param defaultTagName default field name used for selecting tag
     */
    public SimpleTopicSelector(String topicFieldName, String defaultTopicName, String tagFieldName, String defaultTagName) {
        this.topicFieldName = topicFieldName;
        this.defaultTopicName = defaultTopicName;
        this.tagFieldName = tagFieldName;
        this.defaultTagName = defaultTagName;
    }

    @Override
    public String getTopic(Map tuple) {
        if (tuple.containsKey(topicFieldName)) {
            Object topic =  tuple.get(topicFieldName);
            return topic != null ? topic.toString() : defaultTopicName;
        } else {
            LOG.warn("Field {} Not Found. Returning default topic {}", topicFieldName, defaultTopicName);
            return defaultTopicName;
        }
    }

    @Override
    public String getTag(Map tuple) {
        if (tuple.containsKey(tagFieldName)) {
            Object tag = tuple.get(tagFieldName);
            return tag != null ? tag.toString() : defaultTagName;
        } else {
            LOG.warn("Field {} Not Found. Returning default tag {}", tagFieldName, defaultTagName);
            return defaultTagName;
        }
    }
}
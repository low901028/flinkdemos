package com.jdd.streaming.demos;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class KafkaEventSchema implements DeserializationSchema<KafkaStreamingCount.KafkaEvent>, SerializationSchema<KafkaStreamingCount.KafkaEvent> {

    private static final long serialVersionUID = 6154188370181669758L;

    //@Override
    public byte[] serialize(KafkaStreamingCount.KafkaEvent event) {
        return event.toString().getBytes();
    }

    //@Override
    public KafkaStreamingCount.KafkaEvent deserialize(byte[] message) throws IOException {
        return KafkaStreamingCount.KafkaEvent.fromString(new String(message));
    }

    //@Override
    public boolean isEndOfStream(KafkaStreamingCount.KafkaEvent nextElement) {
        return false;
    }

    //@Override
    public TypeInformation<KafkaStreamingCount.KafkaEvent> getProducedType() {
        return TypeInformation.of(KafkaStreamingCount.KafkaEvent.class);
    }
}
package com.jdd.streaming.demos;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Auther: dalan
 * @Date: 19-3-21 16:44
 * @Description:
 */
public class StringLineEventSource  extends RichParallelSourceFunction<String> {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(StringLineEventSource.class);

    public Long latenessMills;
    private volatile boolean running = true;

    public StringLineEventSource(){super();}
    public StringLineEventSource(Long latenessMills){super(); this.latenessMills = latenessMills;}

    private List<String> channelSet = Arrays.asList("a", "b", "c", "d");
    private  List<String> behaviorTypes = Arrays.asList("INSTALL", "OPEN",
                                                "BROWSE", "CLICK",
                                                "PURCHASE", "CLOSE", "UNINSTALL");
    private Random rand = new Random();

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        long numElements = Long.MAX_VALUE;
        long count = 0L;

        while (running && count < numElements){
            String channel = channelSet.get(rand.nextInt(channelSet.size()));
            List<String> event = generateEvent();
            LOGGER.info(event.toString());
            String ts = event.get(0);
            String id = event.get(1);
            String behaviorType = event.get(2);

            String result = StringUtils.join(Arrays.asList(ts, channel, id, behaviorType),"\t");
            ctx.collect(result);

            count += 1;
            TimeUnit.MILLISECONDS.sleep(5L);
        }
    }

    private List<String> generateEvent() {
        Long delayedTimestamp = Instant.ofEpochMilli(System.currentTimeMillis())
                .minusMillis(latenessMills)
                .toEpochMilli();
                 // timestamp, id, behaviorType
        return  Arrays.asList(delayedTimestamp.toString(),
                              UUID.randomUUID().toString(),
                              behaviorTypes.get(rand.nextInt(behaviorTypes.size())));
    }


    @Override
    public void cancel() {
        this.running = false;
    }
}

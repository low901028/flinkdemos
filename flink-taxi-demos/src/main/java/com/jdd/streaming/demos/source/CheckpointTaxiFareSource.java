package com.jdd.streaming.demos.source;

import com.jdd.streaming.demos.entity.TaxiFare;
import com.jdd.streaming.demos.entity.TaxiRide;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * @Auther: dalan
 * @Date: 19-3-29 18:51
 * @Description:
 */
public class CheckpointTaxiFareSource implements SourceFunction<TaxiFare>, ListCheckpointed<Long> {
    private  final String dataFilePath;
    private  final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    // record the number of emitted events
    private long eventCnt = 0;

    public CheckpointTaxiFareSource(String dataFilePath){this(dataFilePath, 1);}
    public CheckpointTaxiFareSource(String dataFilePath, int servingSpeed) {
        this.dataFilePath = dataFilePath;
        this.servingSpeed = servingSpeed;
    }

    @Override public List<Long> snapshotState(long l, long l1) throws Exception {
        return Collections.singletonList(eventCnt);
    }

    @Override public void restoreState(List<Long> list) throws Exception {
        for (Long s: list) {
            this.eventCnt = s;
        }
    }

    @Override public void run(SourceContext<TaxiFare> sourceContext) throws Exception {
        final Object lock = sourceContext.getCheckpointLock();

        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

        Long prevRideTime = null;

        String line;
        long cnt = 0;

        // skip emitted events
        while (cnt < eventCnt && reader.ready() && (line = reader.readLine()) != null) {
            cnt++;
            TaxiFare fare = TaxiFare.fromString(line);
            prevRideTime = getEventTime(fare);
        }

        // emit all subsequent events proportial to their timestamp
        while (reader.ready() && (line = reader.readLine()) != null) {

            TaxiFare fare = TaxiFare.fromString(line);
            long rideTime = getEventTime(fare);

            if (prevRideTime != null) {
                long diff = (rideTime - prevRideTime) / servingSpeed;
                Thread.sleep(diff);
            }

            synchronized (lock) {
                eventCnt++;
                sourceContext.collectWithTimestamp(fare, rideTime);
                sourceContext.emitWatermark(new Watermark(rideTime - 1));
            }

            prevRideTime = rideTime;
        }

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
    }

    @Override public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.gzipStream != null) {
                this.gzipStream.close();
            }
        } catch(IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.gzipStream = null;
        }
    }

    public long getEventTime(TaxiFare fare) {
        return fare.getEventTime();
    }
}

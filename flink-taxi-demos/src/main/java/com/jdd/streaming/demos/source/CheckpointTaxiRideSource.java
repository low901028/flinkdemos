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
 * @Date: 19-3-29 18:52
 * @Description:
 */
public class CheckpointTaxiRideSource implements SourceFunction<TaxiRide>, ListCheckpointed<Long> {
    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    private long eventCnt = 0;

    public CheckpointTaxiRideSource(String dataFilePath){this(dataFilePath,1);}
    public CheckpointTaxiRideSource(String dataFilePath, int servingSpeedFactor) {
        this.dataFilePath = dataFilePath;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override public List<Long> snapshotState(long l, long l1) throws Exception {
        return Collections.singletonList(eventCnt);
    }

    @Override public void restoreState(List<Long> list) throws Exception {
        for (Long s: list) {
            this.eventCnt = s;
        }
    }

    // source 产生数据
    @Override public void run(SourceContext<TaxiRide> sourceContext) throws Exception {
        final Object lock = sourceContext.getCheckpointLock();

        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream,"UTF-8"));

        Long prevRideTime = null;
        String line;
        long cnt = 0;

        // 从gz中读取文件内容 并逐行转为TaxiRide
        while (cnt < eventCnt && reader.ready() && (line = reader.readLine()) != null){
            cnt++;
            TaxiRide  ride =  TaxiRide.fromString(line);
            long rideTime = getEventTime(ride);

            if(prevRideTime != null){ // 控制数据产生的速度: 当前taxiride与上一次taxiride时间差越大 sleep时间就越久
                long diff = (rideTime - prevRideTime) / servingSpeed;
                Thread.sleep(diff);
            }

            synchronized (lock){
                eventCnt++;
                sourceContext.collectWithTimestamp(ride, rideTime);
                sourceContext.emitWatermark(new Watermark(rideTime - 1));
            }
            prevRideTime = rideTime;
        }

        this.reader.close();
        this.reader = null;

        this.gzipStream.close();
        this.gzipStream = null;
    }

    // 放弃数据产生: 常用于释放资源释放
    @Override public void cancel() {
        try{
            if(this.reader != null){
                this.reader.close();
                this.reader = null;
            }
            if(this.gzipStream != null){
                this.gzipStream.close();
                this.gzipStream = null;
            }
        }catch (IOException io){
            throw new RuntimeException("Could not cancel sourcefuntion", io);
        }finally {
            this.reader = null;
            this.gzipStream = null;
        }
    }

    public long getEventTime(TaxiRide ride) {
        return ride.getEventTime();
    }
}

package com.jdd.streaming.demos;

import com.jdd.streaming.demos.entity.TaxiRide;
import com.jdd.streaming.demos.source.TaxiRideSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.jdd.streaming.demos.utils.GeoUtils;

/**
 * @Auther: dalan
 * @Date: 19-3-28 11:50
 * @Description:
 */
public class SimpleFunctions {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleFunctions.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String taxiRide = params.get("taxi-ride-path","/home/wmm/go_bench/flink_sources/nycTaxiRides.gz");
        //String taxiFare = params.get("taxi-fare-path","/home/wmm/go_bench/flink_sources/nycTaxiFares.gz");

        int maxEventDelay = 60;
        int servingSpeedFactor = 600;

        //
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(taxiRide, maxEventDelay, servingSpeedFactor));

        // 实现每个司机在NY范围内 每趟旅程的时间
        rides.flatMap(new FlatMapFunction<TaxiRide, TaxiRide>() {
            @Override public void flatMap(TaxiRide taxiRide, Collector<TaxiRide> out) throws Exception {
                if (GeoUtils.isInNYC(taxiRide.startLon, taxiRide.endLat)){
                    out.collect(taxiRide);
                }
            }
        }).keyBy(new KeySelector<TaxiRide, Long>() {
            @Override public Long getKey(TaxiRide taxiRide) throws Exception {
                return taxiRide.driverId;
            }
        }).flatMap(new FlatMapFunction<TaxiRide, Tuple2<Long, Minutes>>() {
            @Override public void flatMap(TaxiRide taxiRide, Collector<Tuple2<Long, Minutes>> out)
                throws Exception {
                if(!taxiRide.isStart){
                    Interval rideInterval = new Interval(taxiRide.startTime, taxiRide.endTime);
                    Minutes duration = rideInterval.toDuration().toStandardMinutes();
                    out.collect(new Tuple2<Long, Minutes>(taxiRide.driverId, duration));
                }
            }
        }).keyBy(0)
          .maxBy(1)
          .print();

        env.execute("simpel flink operators used.");
    }
}

package com.jdd.streaming.demos;

import com.jdd.streaming.demos.entity.TaxiFare;
import com.jdd.streaming.demos.entity.TaxiRide;
import com.jdd.streaming.demos.source.CheckpointTaxiFareSource;
import com.jdd.streaming.demos.source.CheckpointTaxiRideSource;
import com.jdd.streaming.demos.source.TaxiFareSource;
import com.jdd.streaming.demos.source.TaxiRideSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Auther: dalan
 * @Date: 19-3-29 19:26
 * @Description:
 */
public class TaxiCheckpointDemo {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(TaxiCheckpointDemo.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        final int delay = 60;
        final int servingSpeed = 1800;

        final String ridePath = "/home/wmm/go_bench/flink_sources/nycTaxiRides.gz";
        final String farePath = "/home/wmm/go_bench/flink_sources/nycTaxiFares.gz";

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(ridePath, delay, servingSpeed))
                                        .filter((TaxiRide ride)-> ride.isStart)
                                        .keyBy("rideId");
        DataStream<TaxiFare> fares = env.addSource(new TaxiFareSource(farePath, delay, servingSpeed))
                                        .keyBy("rideId");

        DataStream<Tuple2<TaxiRide, TaxiFare>> rideFares = rides.connect(fares)
                                                                .flatMap(new RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>>(){
                                                                    // 使用内存state方式存储对应的ride或fare
                                                                    private ValueState<TaxiRide> rideStates;
                                                                    private ValueState<TaxiFare> fareStates;

                                                                    @Override
                                                                    public void open(Configuration config) throws Exception {
                                                                        //throw new MissingSolutionException();
                                                                        rideStates = getRuntimeContext().getState(new ValueStateDescriptor<TaxiRide>("ride state", TaxiRide.class));
                                                                        fareStates = getRuntimeContext().getState(new ValueStateDescriptor<TaxiFare>("fare state", TaxiFare.class));
                                                                    }

                                                                    @Override
                                                                    public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
                                                                        TaxiFare fare = fareStates.value();
                                                                        if (fare != null){
                                                                            fareStates.clear();
                                                                            out.collect(new Tuple2<>(ride, fare));
                                                                        } else{
                                                                            rideStates.update(ride);
                                                                        }

                                                                    }

                                                                    @Override
                                                                    public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
                                                                        TaxiRide ride = rideStates.value();
                                                                        if (ride != null){
                                                                            rideStates.clear();

                                                                            out.collect(new Tuple2<>(ride, fare));
                                                                        }else {
                                                                            fareStates.update(fare);
                                                                        }
                                                                    }
                                                                });

        rideFares.print();
        env.execute("rides connect fares demo");

    }

    public static class MissingSolutionException extends Exception {
        public MissingSolutionException() {};

        public MissingSolutionException(String message) {
            super(message);
        };

        public MissingSolutionException(Throwable cause) {
            super(cause);
        }

        public MissingSolutionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

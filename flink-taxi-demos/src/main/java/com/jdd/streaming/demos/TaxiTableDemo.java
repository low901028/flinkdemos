package com.jdd.streaming.demos;

import com.jdd.streaming.demos.entity.TaxiFare;
import com.jdd.streaming.demos.entity.TaxiRide;
import com.jdd.streaming.demos.source.TaxiFareSource;
import com.jdd.streaming.demos.source.TaxiRideSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Auther: dalan
 * @Date: 19-3-28 10:45
 * @Description:
 */
public class TaxiTableDemo {
    /** logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(TaxiTableDemo.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String taxiRide = params.get("taxi-ride-path","/home/wmm/go_bench/flink_sources/nycTaxiRides.gz");
        String taxiFare = params.get("taxi-fare-path","/home/wmm/go_bench/flink_sources/nycTaxiFares.gz");

        //
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        final int maxEventDelay = 60;
        final int servingSpeedFactor = 600;

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(taxiRide, maxEventDelay, servingSpeedFactor));
        DataStream<TaxiFare> fares = env.addSource(new TaxiFareSource(taxiFare, maxEventDelay, servingSpeedFactor));
        Table rideTable = tEnv.fromDataStream(rides);
        Table fareTable = tEnv.fromDataStream(fares);

        // 注册DataStream到TableEnvironment
        //tEnv.registerDataStream("rides", rides);
        //tEnv.registerDataStream("fares", fares);

//        Table result = tEnv.sqlQuery("SELECT * FROM " + fareTable)
//            //.leftOuterJoin(fareTable,"rideId")
//        ;
//
//        //result.printSchema();
//        tEnv.toAppendStream(result, TaxiFare.class).print();

        Table result = tEnv.sqlQuery("SELECT * FROM " + rideTable);
        result.printSchema();
        tEnv.toAppendStream(result, TaxiRide.class).print();
        env.execute("taxi table demo");
    }
}

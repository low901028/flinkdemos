package com.jdd.streaming.demos;

import com.jdd.streaming.demos.entity.TaxiRide;
import com.jdd.streaming.demos.source.TaxiRideSource;
import com.jdd.streaming.demos.utils.GeoUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 * @Auther: dalan
 * @Date: 19-3-22 15:36
 * @Description:
 */
public class SimpleESStream {
    // main
    public static void main(String[] args) throws Exception {
         final ParameterTool params = ParameterTool.fromArgs(args);
         String data = params.get("data","/home/wmm/go_bench/flink_sources/nycTaxiRides.gz");
         int maxServingDelay = 60;
         int servingSpeedFactor = 600;

         int countWindowLength = 15;  // 窗口大小
         int countWindowFrequency = 5; // 每5min计算一次
         int earlyCountThreshold = 50;

         boolean writeToElasticsearch = true;
         String elasticsearchHost = "";
         int elasticsearchPort = 9300;

         final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(data, maxServingDelay, servingSpeedFactor));

        DataStream<TaxiRide> cleansedRides = rides
                .filter(r->!r.isStart)
                .filter(r -> GeoUtils.isInNYC(r.startLon, r.startLat));

        DataStream<Tuple2<Integer, Short>> cellIds = cleansedRides
                .map(
                    new MapFunction<TaxiRide, Tuple2<Integer, Short>>() {
                        @Override
                        public Tuple2<Integer, Short> map(TaxiRide r) throws Exception {
                            return new Tuple2<Integer, Short>(GeoUtils.mapToGridCell(r.startLon, r.startLat), r.passengerCnt);
                        }
                    }
                );

        cellIds.print();

        DataStream<Tuple3<Integer, Long, Integer>> passengerCnts = cellIds
                .keyBy(0)
                .timeWindow(Time.minutes(countWindowLength), Time.minutes(countWindowFrequency))
                .apply(
                        new WindowFunction<Tuple2<Integer, Short>, Tuple3<Integer, Long, Integer>, Tuple, TimeWindow>() {
                            @Override
                            public void apply(Tuple cell, TimeWindow window, Iterable<Tuple2<Integer, Short>> events, Collector<Tuple3<Integer, Long, Integer>> out) throws Exception {
                                Integer count = 0;
                                Iterator<Tuple2<Integer, Short>> iter = events.iterator();

                                while (iter.hasNext()) {
                                    Tuple2<Integer, Short> t = iter.next();
                                    count += t.f1.intValue();
                                }
                                out.collect(new Tuple3<Integer, Long, Integer>(cell.getField(0), window.getEnd(), count));
                            }
                        }

                );

        DataStream<Tuple4<Integer, Long, Tuple2<Double,Double>, Integer>> cntByLocaltion = passengerCnts
                .map(
                        new MapFunction<Tuple3<Integer, Long, Integer>, Tuple4<Integer, Long, Tuple2<Double,Double>, Integer>>() {
                            @Override
                            public Tuple4<Integer, Long, Tuple2<Double,Double>, Integer> map(Tuple3<Integer, Long, Integer> r) throws Exception {
                                return (new Tuple4<Integer, Long, Tuple2<Double,Double>, Integer>(r.f0, r.f1, getGridCellCenter(r.f0), r.f2));
                            }
                        }
                );

        cntByLocaltion.print();
        if(writeToElasticsearch){
            List<HttpHost> httpHosts = new ArrayList<>();
            httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

            Map<String, String> config = new HashMap<>();
            config.put("bulk.flush.max.actions", "1");

            // elasticsearch sink
            ElasticsearchSink.Builder<Tuple4<Integer, Long, Tuple2<Double,Double>, Integer>> builder = new ElasticsearchSink.Builder<Tuple4<Integer, Long, Tuple2<Double,Double>, Integer>>(
                    httpHosts,
                    new ElasticsearchSinkFunction<Tuple4<Integer, Long, Tuple2<Double,Double>, Integer>>() {
                        public IndexRequest createIndexRequest(Tuple4<Integer, Long, Tuple2<Double,Double>, Integer> element) {
                            Map<String, Object> json = new HashMap<>();
                            json.put("localtion", (element.f2.f0+","+ element.f2.f1));
                            json.put("time", element.f1);
                            json.put("cnt", element.f3);

                            return Requests.indexRequest()
                                    .index("taxi_ride_idx")
                                    .type("test_type")
                                    .source(json);
                        }

                        @Override
                        public void process(Tuple4<Integer, Long, Tuple2<Double,Double>, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                            indexer.add(createIndexRequest(element));
                        }
                    });

            builder.setBulkFlushMaxActions(1);
            cntByLocaltion.addSink(builder.build());
        }

        env.execute("a simple es stream demo ");
    }

    // 辅助参数
    public static double LonEast = -73.7;
    public static double LonWest = -74.05;
    public static double LatNorth = 41.0;
    public static double LatSouth = 40.5;

    public static double LonWidth = 74.05 - 73.7;
    public static double LatHeight = 41.0 - 40.5;

    public static double DeltaLon = 0.0014;
    public static double DeltaLat = 0.00125;

    public static int CellCntX = 250;
    public static double CellCntY = 400;

    private static Tuple2<Double, Double> getGridCellCenter(int gridCellId){
        int xIndex  = gridCellId % CellCntX;
        double lon = (Math.abs(LonWest) - (xIndex * DeltaLon) - (DeltaLon / 2)) * -1.0f;

        int yIndex = (gridCellId - xIndex) / CellCntX;
        double lat = (LatNorth - (yIndex * DeltaLat) - (DeltaLat / 2));

        return  new Tuple2<Double, Double>(lon, lat);
    }
}

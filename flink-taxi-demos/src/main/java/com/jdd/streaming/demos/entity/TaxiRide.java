package com.jdd.streaming.demos.entity;

import com.jdd.streaming.demos.utils.GeoUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Locale;

/**
 * @Auther: dalan
 * @Date: 19-3-18 15:23
 * @Description:
 */
public class TaxiRide implements Comparable<TaxiRide>, Serializable {

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public TaxiRide() {
        this.startTime = new DateTime();
        this.endTime = new DateTime();
    }

    public TaxiRide(long rideId, boolean isStart, DateTime startTime, DateTime endTime,
                    float startLon, float startLat, float endLon, float endLat,
                    short passengerCnt, long taxiId, long driverId) {

        this.rideId = rideId;
        this.isStart = isStart;
        this.startTime = startTime;
        this.endTime = endTime;
        this.startLon = startLon;
        this.startLat = startLat;
        this.endLon = endLon;
        this.endLat = endLat;
        this.passengerCnt = passengerCnt;
        this.taxiId = taxiId;
        this.driverId = driverId;
    }

    public long rideId;
    public boolean isStart;
    public DateTime startTime;
    public DateTime endTime;
    public float startLon;
    public float startLat;
    public float endLon;
    public float endLat;
    public int passengerCnt;
    public long taxiId;
    public long driverId;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideId).append(",");
        sb.append(isStart ? "START" : "END").append(",");
        sb.append(startTime.toString(timeFormatter)).append(",");
        sb.append(endTime.toString(timeFormatter)).append(",");
        sb.append(startLon).append(",");
        sb.append(startLat).append(",");
        sb.append(endLon).append(",");
        sb.append(endLat).append(",");
        sb.append(passengerCnt).append(",");
        sb.append(taxiId).append(",");
        sb.append(driverId);

        return sb.toString();
    }

    public static TaxiRide fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 11) {
            throw new RuntimeException("Invalid record: " + line);
        }

        TaxiRide ride = new TaxiRide();

        try {
            ride.rideId = Long.parseLong(tokens[0]);

            switch (tokens[1]) {
                case "START":
                    ride.isStart = true;
                    ride.startTime = DateTime.parse(tokens[2], timeFormatter);
                    ride.endTime = DateTime.parse(tokens[3], timeFormatter);
                    break;
                case "END":
                    ride.isStart = false;
                    ride.endTime = DateTime.parse(tokens[2], timeFormatter);
                    ride.startTime = DateTime.parse(tokens[3], timeFormatter);
                    break;
                default:
                    throw new RuntimeException("Invalid record: " + line);
            }

            ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
            ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
            ride.passengerCnt = Short.parseShort(tokens[8]);
            ride.taxiId = Long.parseLong(tokens[9]);
            ride.driverId = Long.parseLong(tokens[10]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    // sort by timestamp,
    // putting START events before END events if they have the same timestamp
    @Override
    public int compareTo(TaxiRide other) {
        if (other == null) {
            return 1;
        }
        int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
        if (compareTimes == 0) {
            if (this.isStart == other.isStart) {
                return 0;
            }
            else {
                if (this.isStart) {
                    return -1;
                }
                else {
                    return 1;
                }
            }
        }
        else {
            return compareTimes;
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiRide &&
                this.rideId == ((TaxiRide) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

    public long getEventTime() {
        if (isStart) {
            return startTime.getMillis();
        }
        else {
            return endTime.getMillis();
        }
    }

    public double getEuclideanDistance(double longitude, double latitude) {
        if (this.isStart) {
            return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.startLon, this.startLat);
        } else {
            return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.endLon, this.endLat);
        }
    }



    public static class EnrichedRide extends TaxiRide{
        public int startCell;
        public int endCell;

        public EnrichedRide(){}
        public EnrichedRide(TaxiRide ride){
            this.rideId = ride.rideId;
            this.isStart = ride.isStart;
            this.startTime = ride.startTime;
            this.endTime = ride.endTime;
            this.startLon = ride.startLon;
            this.startLat = ride.startLat;
            this.endLon = ride.endLon;
            this.endLat = ride.endLat;
            this.passengerCnt = ride.passengerCnt;
            this.taxiId = ride.taxiId;
            this.driverId = ride.driverId;

            this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
            this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
        }

        @Override
        public String toString() {
            return super.toString() + "," +
                    Integer.toString(this.startCell) + "," +
                    Integer.toString(this.endCell);
        }
    }


}



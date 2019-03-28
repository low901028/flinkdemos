package com.jdd.streaming.demos.entity;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

/**
 * @Auther: dalan
 * @Date: 19-3-18 15:23
 * @Description:
 */
public class TaxiFare implements Serializable {
    private static transient DateTimeFormatter format =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public TaxiFare(){
        this.startTime = new DateTime();
    }

    public TaxiFare(long rideId, long taxiId, long driverId, DateTime startTime, String paymentType, float tip, float tolls, float totalFare) {

        this.rideId = rideId;
        this.taxiId = taxiId;
        this.driverId = driverId;
        this.startTime = startTime;
        this.paymentType = paymentType;
        this.tip = tip;
        this.tolls = tolls;
        this.totalFare = totalFare;
    }

    public long rideId;
    public long taxiId;
    public long driverId;
    public DateTime startTime;
    public String paymentType;
    public float tip;
    public float tolls;
    public float totalFare;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideId).append(",");
        sb.append(taxiId).append(",");
        sb.append(driverId).append(",");
        sb.append(startTime.toString(format)).append(",");
        sb.append(paymentType).append(",");
        sb.append(tip).append(",");
        sb.append(tolls).append(",");
        sb.append(totalFare);

        return sb.toString();
    }

    // 将记录转为entity
    public static TaxiFare fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 8) {
            throw new RuntimeException("Invalid record: " + line);
        }

        TaxiFare ride = new TaxiFare();

        try {
            ride.rideId = Long.parseLong(tokens[0]);
            ride.taxiId = Long.parseLong(tokens[1]);
            ride.driverId = Long.parseLong(tokens[2]);
            ride.startTime = DateTime.parse(tokens[3], format);
            ride.paymentType = tokens[4];
            ride.tip = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            ride.tolls = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            ride.totalFare = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TaxiFare &&
                this.rideId == ((TaxiFare)obj).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

    public long getEventTime(){
        return startTime.getMillis();
    }
}

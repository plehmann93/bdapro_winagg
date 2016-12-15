package de.tuberlin.source;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

/**
 * Created by patrick on 15.12.16.
 */


import java.util.Locale;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

    public class TaxiRide {
        private static transient DateTimeFormatter timeFormatter;
        public long rideId;
        public boolean isStart;
        public DateTime startTime;
        public DateTime endTime;
        public float startLon;
        public float startLat;
        public float endLon;
        public float endLat;
        public short passengerCnt;

        public TaxiRide() {
        }

        public TaxiRide(long rideId, boolean isStart, DateTime startTime, DateTime endTime, float startLon, float startLat, float endLon, float endLat, short passengerCnt) {
            this.rideId = rideId;
            this.isStart = isStart;
            this.startTime = startTime;
            this.endTime = endTime;
            this.startLon = startLon;
            this.startLat = startLat;
            this.endLon = endLon;
            this.endLat = endLat;
            this.passengerCnt = passengerCnt;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(this.rideId).append(",");
            sb.append(this.isStart?"START":"END").append(",");
            if(this.isStart) {
                sb.append(this.startTime.toString(timeFormatter)).append(",");
                sb.append(this.endTime.toString(timeFormatter)).append(",");
            } else {
                sb.append(this.endTime.toString(timeFormatter)).append(",");
                sb.append(this.startTime.toString(timeFormatter)).append(",");
            }

            sb.append(this.startLon).append(",");
            sb.append(this.startLat).append(",");
            sb.append(this.endLon).append(",");
            sb.append(this.endLat).append(",");
            sb.append(this.passengerCnt);
            return sb.toString();
        }

        public static com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide fromString(String line) {
            String[] tokens = line.split(",");
            if(tokens.length != 9) {
                throw new RuntimeException("Invalid record: " + line);
            } else {
                com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide ride = new com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide();

                try {
                    ride.rideId = Long.parseLong(tokens[0]);
                    String nfe = tokens[1];
                    byte var4 = -1;
                    switch(nfe.hashCode()) {
                        case 68795:
                            if(nfe.equals("END")) {
                                var4 = 1;
                            }
                            break;
                        case 79219778:
                            if(nfe.equals("START")) {
                                var4 = 0;
                            }
                    }

                    switch(var4) {
                        case 0:
                            ride.isStart = true;
                            ride.startTime = DateTime.parse(tokens[2], timeFormatter);
                            ride.endTime = DateTime.parse(tokens[3], timeFormatter);
                            break;
                        case 1:
                            ride.isStart = false;
                            ride.endTime = DateTime.parse(tokens[2], timeFormatter);
                            ride.startTime = DateTime.parse(tokens[3], timeFormatter);
                            break;
                        default:
                            throw new RuntimeException("Invalid record: " + line);
                    }

                    ride.startLon = tokens[4].length() > 0?Float.parseFloat(tokens[4]):0.0F;
                    ride.startLat = tokens[5].length() > 0?Float.parseFloat(tokens[5]):0.0F;
                    ride.endLon = tokens[6].length() > 0?Float.parseFloat(tokens[6]):0.0F;
                    ride.endLat = tokens[7].length() > 0?Float.parseFloat(tokens[7]):0.0F;
                    ride.passengerCnt = Short.parseShort(tokens[8]);
                    return ride;
                } catch (NumberFormatException var5) {
                    throw new RuntimeException("Invalid record: " + line, var5);
                }
            }
        }

        public boolean equals(Object other) {
            return other instanceof com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide && this.rideId == ((com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide)other).rideId;
        }

        public int hashCode() {
            return (int)this.rideId;
        }

        static {
            timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();
        }

}


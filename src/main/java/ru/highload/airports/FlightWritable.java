package ru.highload.airports;

import org.apache.hadoop.io.Text;

public class FlightWritable {
    private int destAirportID;
    private float delayTime;

    public FlightWritable(Text text) {
        String[] cols = text.toString().split(",");
        String destAirport = cols[14].replaceAll("\"", "");
        String delay = cols[17].replaceAll("\"", "");
        if (destAirport.equals("DEST_AIRPORT_ID")) {
            destAirportID = -1;
        } else {
            destAirportID = Integer.parseInt(destAirport);
        }
        if (delay.equals("ARR_DELAY")) {
            delayTime = -1;
        } else {
            delayTime = Float.parseFloat(destAirport);
        }
    }


}

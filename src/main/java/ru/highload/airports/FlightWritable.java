package ru.highload.airports;

import org.apache.hadoop.io.Text;

public class FlightWritable {
    int destAirportID;

    public FlightWritable(Text text) {
        String[] cols = text.toString().split(",");
        String destAirport = cols[14].replaceAll("\"", "");
        String delay = cols[17].replaceAll("\"", "");
        if (destAirport.equals("DEST_AIRPORT_ID")) {
            destAirportID = -1;
        } else {
            destAirportID = Integer.parseInt(destAirport);
        }
        if (destAirport.equals("DEST_AIRPORT_ID")) {
            destAirportID = -1;
        } else {
            destAirportID = Integer.parseInt(destAirport);
        }
    }
}

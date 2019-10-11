package ru.highload.airports;

import org.apache.hadoop.io.Text;

public class FlightWritable {
    int destAirportID
    public FlightWritable(Text text){
        String[] cols = text.toString().split(",");
        String destAirport = cols[14].replaceAll("\"", "");
        if(destAirport == "")
    }
}

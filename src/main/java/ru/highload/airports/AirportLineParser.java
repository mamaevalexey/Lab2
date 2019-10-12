package ru.highload.airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportLineParser {
    private int airportID;
    private String airportName;

    public AirportLineParser(Text text) {
        String[] cols = text.toString().split(",");
        String airport = cols[0].replaceAll("\"", "");
        String name = cols[1].replaceAll("\"", "");

        if (cols.length > 2) {
            name += cols[2].replaceAll("\"", "");
        }

        if (airport.equals("Code")) {
            airportID = -1;
        } else {
            airportID = Integer.parseInt(airport);
        }

        if (name.equals("Description")) {
            airportName = "";
        } else {
            airportName = name;
        }
    }

    public String getAirportName() {
        return airportName;
    }

    public int getAirportID() {
        return airportID;
    }
}

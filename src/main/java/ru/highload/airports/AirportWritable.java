package ru.highload.airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritable implements Writable {
    private int airportID;
    private String airportName;

    public AirportWritable(Text text) {
        String[] cols = text.toString().split(",");
        String airport = cols[0].replaceAll("\"", "");
        String name = cols[1].replaceAll("\"", "");

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

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(airportID);
        dataOutput.writeUTF(airportName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportID = dataInput.readInt();
        airportName = dataInput.readUTF();
    }
}

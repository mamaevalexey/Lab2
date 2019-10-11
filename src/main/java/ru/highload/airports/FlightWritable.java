package ru.highload.airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightWritable extends Writable {
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

    public float getDelayTime() {
        return delayTime;
    }

    public int getDestAirportID() {
        return destAirportID;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}

package ru.highload.airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightWritable implements Writable {
    private static final float EPS = 1e-9f;
    private static final int DEST_AIRPORT_INDEX = 14;
    private static final int DELAY_INDEX = 18;
    private static final int CANCELLED_INDEX = 19;

    private static final String DEST_AIRPORT_COLUMN_NAME = "DEST_AIRPORT_ID";
    private static final String DELAY_COLUMN_NAME = "ARR_DELAY_NEW";

    private int destAirportID;
    private float delayTime;

    public FlightWritable(Text text) {
        String[] cols = text.toString().split(",");
        String destAirport = cols[DEST_AIRPORT_INDEX].replaceAll("\"", "");
        String delay = cols[DELAY_INDEX].replaceAll("\"", "");
        String cancelled = cols[CANCELLED_INDEX].replaceAll("\"", "");

        if (destAirport.equals(DEST_AIRPORT_COLUMN_NAME)) {
            destAirportID = -1;
        } else {
            destAirportID = Integer.parseInt(destAirport);
        }

        if (delay.equals(DELAY_COLUMN_NAME) || delay.equals("") || Math.abs(Float.parseFloat(cancelled) - 1.f) < EPS) {
            delayTime = -1.f;
        } else {
            delayTime = Float.parseFloat(delay);
            if (Math.abs(delayTime - 0.f) < EPS)   // delayTime == 0
                delayTime = -1.f;
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
        dataOutput.writeInt(destAirportID);
        dataOutput.writeFloat(delayTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        destAirportID = dataInput.readInt();
        delayTime = dataInput.readFloat();
    }
}

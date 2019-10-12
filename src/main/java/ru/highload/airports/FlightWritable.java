package ru.highload.airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightWritable implements Writable {
    private static final float EPS = 0.00001f;

    private int destAirportID;
    private float delayTime;

    public FlightWritable(Text text) {
        String[] cols = text.toString().split(",");
        String destAirport = cols[14].replaceAll("\"", "");
        String delay = cols[18].replaceAll("\"", "");
        String cancelled = cols[19].replaceAll("\"", "");

        if (destAirport.equals("DEST_AIRPORT_ID")) {
            destAirportID = -1;
        } else {
            destAirportID = Integer.parseInt(destAirport);
        }

        if (delay.equals("ARR_DELAY_NEW") || delay.equals("") || Math.abs(Float.parseFloat(cancelled) - 1.f) < EPS) {
            delayTime = -1.f;
        } else {
            delayTime = Float.parseFloat(delay);
            if (Math.abs(delayTime - 0.f) < EPS)
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

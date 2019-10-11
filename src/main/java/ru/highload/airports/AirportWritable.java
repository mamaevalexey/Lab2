package ru.highload.airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritable implements Writable {
    private int destAirportID;
    private float delayTime;

    public AirportWritable(Text text) {
        String[] cols = text.toString().split(",");
        String airport = cols[0].replaceAll("\"", "");
        String delay = cols[17].replaceAll("\"", "");
        if (airport.equals("Code")) {
            destAirportID = -1;
        } else {
            destAirportID = Integer.parseInt(airport);
        }
        if (delay.equals("ARR_DELAY")) {
            delayTime = -1;
        } else {
            delayTime = Float.parseFloat(airport);
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

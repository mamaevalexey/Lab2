package ru.highload.airports;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportIDWritable implements WritableComparable<AirportIDWritable> {
    private int airportID;
    private int dataSet; // 0 for 

    public AirportIDWritable(int airportID) {
        this.airportID = airportID;
    }

    public int getAirportID() {
        return airportID;
    }

    @Override
    public int compareTo(AirportIDWritable o) {
        return airportID - o.airportID;
        //TODO: DATA?
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(airportID);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportID = dataInput.readInt();
    }
}

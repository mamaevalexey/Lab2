package ru.highload.airports;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportIDWritableComparable implements WritableComparable<AirportIDWritableComparable> {
    private int airportID;
    private int dataSet; // 0 for airport, 1 for flight

    public AirportIDWritableComparable(int airportID) {
        this.airportID = airportID;
    }

    public int getAirportID() {
        return airportID;
    }

    @Override
    public int compareTo(AirportIDWritableComparable o) {
        if (airportID == o.airportID)
            return dataSet - o.dataSet;
        return airportID - o.airportID;
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

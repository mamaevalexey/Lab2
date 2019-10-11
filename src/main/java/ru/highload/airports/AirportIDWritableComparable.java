package ru.highload.airports;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportIDWritableComparable implements WritableComparable<AirportIDWritableComparable> {
    private int airportID;
    private int dataSet; // 0 for airport, 1 for flight

    public AirportIDWritableComparable(int airportID, int dataSet) {
        this.airportID = airportID;
        this.dataSet = dataSet;
    }

    @Override
    public int compareTo(AirportIDWritableComparable o) {
        if (airportID == o.airportID)
            return dataSet - o.dataSet;
        return airportID - o.airportID;
    }

    public int getAirportID() {
        return airportID;
    }

    public int getDataSet() {
        return dataSet;
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

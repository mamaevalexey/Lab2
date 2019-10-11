package ru.highload.airports;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportIDComparator extends WritableComparator {

    protected AirportIDComparator(){
        super(AirportIDWritableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        int leftAirportID = ((AirportIDWritableComparable) a).getAirportID();
        int rightAirportID = ((AirportIDWritableComparable) b).getAirportID();
        return Integer.compare(leftAirportID, rightAirportID);
    }
}

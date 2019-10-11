package ru.highload.airports;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportIDComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(a, b);
    }
}

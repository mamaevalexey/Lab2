package ru.highload.airports;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class AirportPartitioner extends HashPartitioner<AirportIDWritableComparable, Text> {
    @Override
    public int getPartition(AirportIDWritableComparable key, Text value, int numReduceTasks) {
        return super.getPartition(key, value, numReduceTasks);
    }
}
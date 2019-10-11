package ru.highload.airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable, Text, FlightWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        FlightWritable flight = new FlightWritable(value);
        if (flight.getDestAirportID() != -1 && flight.getDelayTime() != -1) {
            context.write();
        }
    }
}

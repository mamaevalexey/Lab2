package ru.highload.airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, AirportIDWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FlightWritable flight = new FlightWritable(value);
        if (flight.getDestAirportID() != -1 && flight.getDelayTime() != -1) {
            context.write(new AirportIDWritableComparable(flight.getDestAirportID(), 1),
                    new Text(String.valueOf(flight.getDelayTime())));
        }
    }
}

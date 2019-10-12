package ru.highload.airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, AirportIDWritableComparable, Text> {
    private static final float EPS = 0.00001f;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FlightWritable flight = new FlightWritable(value);

        //                                     delayTime != -1
        if (flight.getDestAirportID() != -1 && Math.abs(flight.getDelayTime() + 1.f) >= EPS) {
            context.write(new AirportIDWritableComparable(flight.getDestAirportID(), 1),
                    new Text(String.valueOf(flight.getDelayTime())));
        }
    }
}

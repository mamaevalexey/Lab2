package ru.highload.airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, AirportIDWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirportWritable airport = new AirportWritable(value);

        if (airport.getAirportID() != -1 && !airport.getAirportName().equals("")) {
            context.write(new AirportIDWritableComparable(airport.getAirportID(), 0),
                    new Text(airport.getAirportName()));
        }
    }
}

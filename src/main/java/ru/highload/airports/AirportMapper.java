package ru.highload.airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirportMapper extends Mapper<LongWritable, Text, AirportWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context){
        AirportWritable airport = new AirportWritable(value);

        if (airport.getAirportID() != -1)
            context.write(airport, new Text(airport.getAirportName()));
    }
}

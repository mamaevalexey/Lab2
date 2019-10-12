package ru.highload.airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, AirportIDWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String []cols = CSVParser.makeCols(value.toString());
        if (cols.length != 2 && cols.length != 3){
            return;
        }
        if (cols[0].equals("Code")){
            return;
        }
        if (cols[1].equals("Description")){
            return;
        }

        int airportID = Integer.parseInt(cols[0]);
        String airportName = cols[1];

        if (cols.length > 2) {
            airportName += cols[2];
        }

        if (airportID != -1 && !airportName.equals("")) {
            context.write(new AirportIDWritableComparable(airportID, 0),
                    new Text(airportName));
        }
    }
}

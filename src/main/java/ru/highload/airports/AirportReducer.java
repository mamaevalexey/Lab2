package ru.highload.airports;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirportReducer extends Reducer<AirportIDWritableComparable, Text, Text, Text> {

    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text systemInfo = new Text(iter.next());
        while (iter.hasNext()) {
            Text call = iter.next();
            Text outValue = new Text(call.toString() + "\t" + systemInfo.toString());
            context.write(key.getFirst(), outValue);
        }
    }
}
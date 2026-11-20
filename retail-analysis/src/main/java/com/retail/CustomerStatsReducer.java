package com.retail;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Output: key <TAB> total <TAB> num_purchases <TAB> avg <TAB> first <TAB> last */
public class CustomerStatsReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StatsAccumulator acc = new StatsAccumulator();
        for (Text v : values) acc.add(v.toString());
        outValue.set(acc.toFinalValue());
        context.write(key, outValue);
    }
}

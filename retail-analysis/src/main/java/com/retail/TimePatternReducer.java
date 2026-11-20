package com.retail;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Cộng số đếm; dùng làm cả Combiner lẫn Reducer. */
public class TimePatternReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable result = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable v : values) sum += v.get();
        result.set(sum);
        context.write(key, result);
    }
}

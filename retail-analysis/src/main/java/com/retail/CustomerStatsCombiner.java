package com.retail;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Gộp cục bộ phía Mapper; xuất giá trị trung gian (không tính trung bình ở đây). */
public class CustomerStatsCombiner extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StatsAccumulator acc = new StatsAccumulator();
        for (Text v : values) acc.add(v.toString());
        outValue.set(acc.toPartialValue());
        context.write(key, outValue);
    }
}

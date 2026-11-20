package com.retail;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Gộp cục bộ ở phía Mapper. Value: sum,count,min_timestamp,max_timestamp
 * Không tính trung bình ở đây (trung bình của trung bình sẽ sai).
 */
public class CustomerStatsCombiner extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        long count = 0;
        String min = null, max = null;
        for (Text v : values) {
            String[] p = v.toString().split(",", 4);
            sum += Double.parseDouble(p[0]);
            count += Long.parseLong(p[1]);
            if (min == null || p[2].compareTo(min) < 0) min = p[2];
            if (max == null || p[3].compareTo(max) > 0) max = p[3];
        }
        outValue.set(sum + "," + count + "," + min + "," + max);
        context.write(key, outValue);
    }
}

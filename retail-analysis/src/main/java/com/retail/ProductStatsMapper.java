package com.retail;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Như CustomerStatsMapper nhưng khóa là product_id. Dùng chung Combiner/Reducer. */
public class ProductStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] r = CustomerStatsMapper.parseLine(value.toString(), 3);
        if (r == null) {
            context.getCounter(CustomerStatsMapper.Counters.INVALID_RECORDS).increment(1);
            return;
        }
        if (r.length == 0) {
            context.getCounter(CustomerStatsMapper.Counters.SKIPPED_NON_PURCHASE).increment(1);
            return;
        }
        outKey.set(r[0]);
        outValue.set(r[1]);
        context.write(outKey, outValue);
        context.getCounter(CustomerStatsMapper.Counters.PURCHASE_RECORDS).increment(1);
    }
}

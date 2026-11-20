package com.retail;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input : timestamp,event_type,user_id,product_id,category_id,category_code,brand,price,user_session
 * Output: user_id -> "amount,1,timestamp,timestamp"   (chỉ với event_type = purchase)
 */
public class CustomerStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    public enum Counters { INVALID_RECORDS, SKIPPED_NON_PURCHASE, PURCHASE_RECORDS }

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] f = value.toString().split(",", -1);
        if (f.length != 9) {
            context.getCounter(Counters.INVALID_RECORDS).increment(1);
            return;
        }
        if (!"purchase".equals(f[1])) {
            context.getCounter(Counters.SKIPPED_NON_PURCHASE).increment(1);
            return;
        }
        String timestamp = f[0];
        String userId = f[2];
        double price;
        try {
            price = Double.parseDouble(f[7]);
        } catch (NumberFormatException e) {
            context.getCounter(Counters.INVALID_RECORDS).increment(1);
            return;
        }
        if (userId.isEmpty() || timestamp.isEmpty() || price <= 0) {
            context.getCounter(Counters.INVALID_RECORDS).increment(1);
            return;
        }
        outKey.set(userId);
        outValue.set(price + ",1," + timestamp + "," + timestamp);
        context.write(outKey, outValue);
        context.getCounter(Counters.PURCHASE_RECORDS).increment(1);
    }
}

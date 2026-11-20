package com.retail;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Output: customer_id <TAB> total_spent <TAB> num_purchases <TAB> avg_amount <TAB> first_purchase <TAB> last_purchase
 */
public class CustomerStatsReducer extends Reducer<Text, Text, Text, Text> {

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
        double avg = sum / count;
        outValue.set(String.format(Locale.US, "%.2f\t%d\t%.2f\t%s\t%s", sum, count, avg, min, max));
        context.write(key, outValue);
    }
}

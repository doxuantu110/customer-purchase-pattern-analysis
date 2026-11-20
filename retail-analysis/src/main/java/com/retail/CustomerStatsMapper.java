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

    private static final String[] SKIP = new String[0];

    /**
     * @param keyIndex vị trí cột dùng làm khóa (2 = user_id, 3 = product_id)
     * @return null nếu dòng lỗi; mảng rỗng nếu không phải purchase;
     *         ngược lại {khóa, "amount,1,timestamp,timestamp"}
     */
    public static String[] parseLine(String line, int keyIndex) {
        String[] f = line.split(",", -1);
        if (f.length != 9) return null;
        if (!"purchase".equals(f[1])) return SKIP;
        String timestamp = f[0];
        String key = f[keyIndex];
        double price;
        try {
            price = Double.parseDouble(f[7]);
        } catch (NumberFormatException e) {
            return null;
        }
        if (key.isEmpty() || timestamp.isEmpty() || !(price > 0)) return null;
        return new String[] { key, price + ",1," + timestamp + "," + timestamp };
    }

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] r = parseLine(value.toString(), 2);
        if (r == null) {
            context.getCounter(Counters.INVALID_RECORDS).increment(1);
            return;
        }
        if (r.length == 0) {
            context.getCounter(Counters.SKIPPED_NON_PURCHASE).increment(1);
            return;
        }
        outKey.set(r[0]);
        outValue.set(r[1]);
        context.write(outKey, outValue);
        context.getCounter(Counters.PURCHASE_RECORDS).increment(1);
    }
}

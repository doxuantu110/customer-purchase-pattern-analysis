package com.retail;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Đếm purchase theo (thứ trong tuần 1=Mon..7=Sun, giờ 00-23), theo giờ trong dữ liệu (UTC). */
public class TimePatternMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1);
    private final Text outKey = new Text();

    /** "2019-10-01 07:15:00" -> "2<TAB>07"; ném DateTimeParseException hoặc IndexOutOfBounds nếu sai định dạng. */
    public static String timeKey(String timestamp) {
        int dow = LocalDate.parse(timestamp.substring(0, 10)).getDayOfWeek().getValue();
        String hour = timestamp.substring(11, 13);
        return dow + "\t" + hour;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] r = CustomerStatsMapper.parseLine(value.toString(), 2);
        if (r == null) {
            context.getCounter(CustomerStatsMapper.Counters.INVALID_RECORDS).increment(1);
            return;
        }
        if (r.length == 0) {
            context.getCounter(CustomerStatsMapper.Counters.SKIPPED_NON_PURCHASE).increment(1);
            return;
        }
        String timestamp = r[1].split(",", 4)[2];
        try {
            outKey.set(timeKey(timestamp));
        } catch (DateTimeParseException | IndexOutOfBoundsException e) {
            context.getCounter(CustomerStatsMapper.Counters.INVALID_RECORDS).increment(1);
            return;
        }
        context.write(outKey, ONE);
        context.getCounter(CustomerStatsMapper.Counters.PURCHASE_RECORDS).increment(1);
    }
}

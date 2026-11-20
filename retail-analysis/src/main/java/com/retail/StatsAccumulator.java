package com.retail;

import java.util.Locale;

/**
 * Gộp các giá trị "sum,count,min_timestamp,max_timestamp".
 * Dùng chung cho Combiner và Reducer (và để unit test logic gộp).
 */
public class StatsAccumulator {

    private double sum;
    private long count;
    private String min;
    private String max;

    public void add(String value) {
        String[] p = value.split(",", 4);
        sum += Double.parseDouble(p[0]);
        count += Long.parseLong(p[1]);
        if (min == null || p[2].compareTo(min) < 0) min = p[2];
        if (max == null || p[3].compareTo(max) > 0) max = p[3];
    }

    public double getSum() { return sum; }
    public long getCount() { return count; }
    public String getMin() { return min; }
    public String getMax() { return max; }

    /** Giá trị trung gian (Combiner xuất ra, Reducer đọc lại được). */
    public String toPartialValue() {
        return sum + "," + count + "," + min + "," + max;
    }

    /** total_spent <TAB> num_purchases <TAB> avg_amount <TAB> first <TAB> last */
    public String toFinalValue() {
        double avg = sum / count;
        return String.format(Locale.US, "%.2f\t%d\t%.2f\t%s\t%s", sum, count, avg, min, max);
    }
}

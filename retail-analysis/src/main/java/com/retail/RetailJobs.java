package com.retail;

import org.apache.hadoop.util.ProgramDriver;

/**
 * Điểm vào duy nhất của JAR:
 *   hadoop jar retail-analysis.jar customerstats <in> <out> [reducers]
 *   hadoop jar retail-analysis.jar timepattern   <in> <out> [reducers]
 *   hadoop jar retail-analysis.jar productstats  <in> <out> [reducers]
 */
public class RetailJobs {
    public static void main(String[] args) throws Throwable {
        ProgramDriver pgd = new ProgramDriver();
        pgd.addClass("customerstats", CustomerStatsDriver.class, "Chỉ số mua hàng theo khách");
        pgd.addClass("timepattern", TimePatternDriver.class, "Số giao dịch theo thứ và giờ");
        pgd.addClass("productstats", ProductStatsDriver.class, "Doanh thu và số lượt mua theo sản phẩm");
        System.exit(pgd.run(args));
    }
}

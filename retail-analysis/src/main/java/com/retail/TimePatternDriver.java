package com.retail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Output: day_of_week(1=Mon) <TAB> hour <TAB> num_purchases */
public class TimePatternDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: timepattern <input> <output> [numReducers]");
            return 2;
        }
        Job job = Job.getInstance(getConf(), "time-pattern");
        job.setJarByClass(TimePatternDriver.class);
        job.setMapperClass(TimePatternMapper.class);
        job.setCombinerClass(TimePatternReducer.class);
        job.setReducerClass(TimePatternReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        if (args.length >= 3) job.setNumReduceTasks(Integer.parseInt(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TimePatternDriver(), args));
    }
}

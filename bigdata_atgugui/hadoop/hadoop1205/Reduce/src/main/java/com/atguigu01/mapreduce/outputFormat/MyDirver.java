package com.atguigu01.mapreduce.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by chenqinping on 2019/3/6 10 33
 */
public class MyDirver {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MyDirver.class);

        job.setOutputFormatClass(MyOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\mapreduce\\log"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mapreduce\\log1"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

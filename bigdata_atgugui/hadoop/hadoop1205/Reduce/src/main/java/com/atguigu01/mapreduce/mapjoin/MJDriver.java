package com.atguigu01.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Create by chenqinping on 2019/3/6 15 10
 */
public class MJDriver {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MJDriver.class);
        job.setMapperClass(MJMapper.class);
        job.setNumReduceTasks(0);

        job.addCacheFile(URI.create("file:///D:/mapreduce/jon3/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("D:\\mapreduce\\join"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mapreduce\\join2"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}

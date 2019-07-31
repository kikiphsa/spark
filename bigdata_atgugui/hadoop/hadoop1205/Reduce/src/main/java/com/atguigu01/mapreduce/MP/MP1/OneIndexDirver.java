package com.atguigu01.mapreduce.MP.MP1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Create by chenqinping on 2019/3/6 14 37
 */
public class OneIndexDirver {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(OneIndexDirver.class);

        job.setMapperClass(OneIndexMapper.class);
        job.setReducerClass(OneIndexReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\mapreduce\\MR\\1"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mapreduce\\MR\\11"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);

    }
}

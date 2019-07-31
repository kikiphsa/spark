package com.atguigu01.mapreduce.MP.MP1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Create by chenqinping on 2019/3/6 14 37
 */
public class TwoIndexDirver {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TwoIndexDirver.class);

        job.setMapperClass(TWOIndexMapper.class);
        job.setReducerClass(TwoReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\mapreduce\\MR\\11"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mapreduce\\MR\\111"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);

    }
}

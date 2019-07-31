package com.atguigu01.mapreduce.join.copy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by chenqinping on 2019/3/6 11 47
 */
public class CopyRJDirver {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(CopyRJDirver.class);

        job.setMapperClass(CopyRIMapper.class);
        job.setReducerClass(CopyRJReduce.class);

        job.setMapOutputKeyClass(CopyOrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(CopyOrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(CopyComparator.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\mapreduce\\join"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mapreduce\\join1"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}

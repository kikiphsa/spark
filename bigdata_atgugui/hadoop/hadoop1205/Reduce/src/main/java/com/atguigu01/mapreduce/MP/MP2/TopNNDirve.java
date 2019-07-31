package com.atguigu01.mapreduce.MP.MP2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by chenqinping on 2019/3/11 10 25
 */
public class TopNNDirve {

    public static void main(String[] args)throws Exception {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TopNNDirve.class);

        job.setMapperClass(Top10Mapper.class);
        job.setReducerClass(Top10Reduce.class);

        job.setSortComparatorClass(Top10Comparator.class);


        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\mapreduce\\MR\\2"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mapreduce\\MR\\22"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}

package com.atguigu01.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by chenqinping on 2019/3/4 08 40
 */
public class MapDirver {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());

        //设置Class
        job.setJarByClass(MapDirver.class);

        //输入map 和 reduce
        job.setMapperClass(MapMapper.class);
        job.setReducerClass(MapReduce.class);


        //输出map 和reduce
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);


    }
}

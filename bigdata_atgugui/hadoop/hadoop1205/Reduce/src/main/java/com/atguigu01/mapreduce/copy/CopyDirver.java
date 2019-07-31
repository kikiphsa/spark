package com.atguigu01.mapreduce.copy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by chenqinping on 2019/3/5 11 24
 */
public class CopyDirver {

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(CopyDirver.class);


        job.setMapperClass(CopyMapper.class);
        job.setReducerClass(CopyReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Copy1Bean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Copy1Bean.class);



        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("n8iemdie ");
    }
}

package com.atguigu01.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"d:/inputlog", "d:/output1"};

        //获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //加载jar包
        job.setJarByClass(LogDriver.class);
        //关联map
        job.setMapperClass(LogMapper.class);

        //4.设置最终输出关系
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交
        job.waitForCompletion(true);
    }
}

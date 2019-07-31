package com.atguigu01.mapreduce.MP.MP3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Create by chenqinping on 2019/3/11 08 56
 */
public class OneShareFriendsDirver {


    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(OneShareFriendsDirver.class);

        job.setMapperClass(OneShareFriendsMapper.class);
        job.setReducerClass(OneShareFriendsReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\mapreduce\\MR\\MR2"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\mapreduce\\MR\\MR22"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}

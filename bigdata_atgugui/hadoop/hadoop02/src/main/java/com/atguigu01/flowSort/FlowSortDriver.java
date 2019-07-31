package com.atguigu01.flowSort;

import com.atguigu01.flowcount.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Create by chenqingping on ${DATA}
 */
public class FlowSortDriver {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowSortDriver.class);

        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReduce.class);

        job.setMapOutputKeyClass(com.atguigu01.flowcount.FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //添加分区
//        job.setPartitionerClass(ProvincePartion.class);
//        job.setNumReduceTasks(6);



        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

package com.atguigu01.mapreduce.writablecomparble;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/5 10 19
 */
public class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean flowBean = new FlowBean();
    Text val = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split("\t");

        val.set(words[0]);
        long upFlow = Long.parseLong(words[1]);
        long downFlow = Long.parseLong(words[2]);

        flowBean.set(upFlow, downFlow);

        context.write(flowBean, val);
    }
}

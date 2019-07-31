package com.atguigu01.mapreduce.MP.MP2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/11 11 12
 */
public class Top10Mapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean flowBean = new FlowBean();
    Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String[] fields = value.toString().split("\t");

        phone.set(fields[0]);

        flowBean.setUpFlow(Long.parseLong(fields[1]));
        flowBean.setDownFlow(Long.parseLong(fields[2]));
        flowBean.setSumFlow(Long.parseLong(fields[3]));

        context.write(flowBean, phone);
    }
}

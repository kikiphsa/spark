package com.atguigu01.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/4 10 13
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    Text phone= new  Text();
    FlowBean  flowBean= new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        phone.set(fields[1]);
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        flowBean.set(upFlow,downFlow);
        context.write(phone,flowBean);
    }
}

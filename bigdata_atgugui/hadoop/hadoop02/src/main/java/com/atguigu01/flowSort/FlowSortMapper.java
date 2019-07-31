package com.atguigu01.flowSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean bean = new FlowBean();
   Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = line.split("\t");

        String phoneNum = fields[0];

        long upflow = Long.parseLong(fields[1]);
        long downflow = Long.parseLong(fields[2]);
        bean.set(upflow,downflow);
        v.set(phoneNum);
        context.write(bean,v);
    }
}

package com.atguigu01.mapreduce.copy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/5 11 06
 */
public class CopyMapper extends Mapper<LongWritable, Text, Text, Copy1Bean> {

    Text text = new Text();
    Copy1Bean val = new Copy1Bean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        text.set(fields[1]);

        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        val.set(upFlow, downFlow);
        context.write(text, val);

    }
}

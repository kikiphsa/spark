package com.atguigu01.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//atguigu--a.txt	3
//        atguigu	c.txt-->2	b.txt-->2	a.txt-->3

        String line = value.toString();

        String[] words = line.split("--");

        k.set(words[0]);
        v.set(words[1]);
        context.write(k, v);
    }
}

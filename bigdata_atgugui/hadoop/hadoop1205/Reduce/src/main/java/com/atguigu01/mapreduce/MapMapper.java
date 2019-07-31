package com.atguigu01.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/4 08 34
 */
public class MapMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //转化toStrring
        String line = value.toString();

        //切割
        String[] words = line.split(" ");


        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}

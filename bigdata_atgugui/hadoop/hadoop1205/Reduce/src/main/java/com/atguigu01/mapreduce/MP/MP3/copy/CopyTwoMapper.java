package com.atguigu01.mapreduce.MP.MP3.copy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Create by chenqinping on 2019/3/11 18 37
 */
public class CopyTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");

        String fiend = split[0];

        String[] beans = split[1].split("=");

        Arrays.sort(beans);

        for (int i = 0; i < beans.length-1; i++) {
            for (int j = i+1; j < beans.length; j++) {
                context.write(new Text(beans[i] + "-" + beans[j]), new Text(fiend));
            }
        }


    }
}

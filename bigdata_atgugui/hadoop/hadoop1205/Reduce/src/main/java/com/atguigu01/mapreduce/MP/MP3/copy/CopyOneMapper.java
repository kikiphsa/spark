package com.atguigu01.mapreduce.MP.MP3.copy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/11 18 25
 */
public class CopyOneMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(":");

        String fiend = split[0];

        k.set(fiend);
        String[] fields = split[1].split(",");

        for (String v : fields) {

            context.write(new Text(v), k);
        }
    }
}

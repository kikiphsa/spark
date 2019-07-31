package com.atguigu01.mapreduce.MP.MP3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/11 08 49
 */
public class OneShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(":");

        String person = fields[0];

        String[] friends = fields[1].split(",");

        for (String friend : friends) {
            context.write(new Text(friend), new Text(person));
        }
    }
}

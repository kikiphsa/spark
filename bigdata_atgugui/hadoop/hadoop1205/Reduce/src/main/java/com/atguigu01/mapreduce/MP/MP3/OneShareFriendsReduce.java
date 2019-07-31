package com.atguigu01.mapreduce.MP.MP3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/11 08 54
 */
public class OneShareFriendsReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text value : values) {
            sb.append(value).append(",");
        }
        context.write(key, new Text(sb.toString()));
    }
}

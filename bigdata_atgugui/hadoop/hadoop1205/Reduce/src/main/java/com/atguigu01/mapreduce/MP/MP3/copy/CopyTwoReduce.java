package com.atguigu01.mapreduce.MP.MP3.copy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/11 18 42
 */
public class CopyTwoReduce extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        for (Text value : values) {

            sb.append(value).append("++");
        }

        context.write(key,new Text(sb.toString()));

    }
}

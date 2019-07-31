package com.atguigu01.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class TwoIndexReduce extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //atguigu	c.txt-->2	b.txt-->2	a.txt-->3
        StringBuilder sb = new StringBuilder();
        for (Text value :values){
            sb.append(value.toString().replace("\t","-->")+"\t");
        }
        context.write(key,new Text(sb.toString()));
    }
}

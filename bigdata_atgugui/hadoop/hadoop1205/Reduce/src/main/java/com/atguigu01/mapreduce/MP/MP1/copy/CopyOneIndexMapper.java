package com.atguigu01.mapreduce.MP.MP1.copy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/11 11 42
 */
public class CopyOneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();

    IntWritable v = new IntWritable();

    String name;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit fs = (FileSplit) context.getInputSplit();

        name = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");

        for (String word : fields) {
            k.set(word + "--" + name);
            v.set(1);

            context.write(k, v);
        }
    }
}

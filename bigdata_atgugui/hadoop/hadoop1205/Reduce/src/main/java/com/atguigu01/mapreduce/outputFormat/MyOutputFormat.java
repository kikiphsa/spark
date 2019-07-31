package com.atguigu01.mapreduce.outputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/6 10 12
 */
public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        MyRecordWriter myRecordWriter = new MyRecordWriter();
        try {
            myRecordWriter.initialize(job);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return myRecordWriter;
    }
}

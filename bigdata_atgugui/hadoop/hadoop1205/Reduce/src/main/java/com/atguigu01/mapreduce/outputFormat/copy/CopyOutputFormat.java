package com.atguigu01.mapreduce.outputFormat.copy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/6 10 41
 */
public class CopyOutputFormat extends FileOutputFormat<LongWritable, Text> {
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        CopyRecordWrite copyRecordWrite = new CopyRecordWrite();
        copyRecordWrite.initialize(job);

        return copyRecordWrite;
    }
}

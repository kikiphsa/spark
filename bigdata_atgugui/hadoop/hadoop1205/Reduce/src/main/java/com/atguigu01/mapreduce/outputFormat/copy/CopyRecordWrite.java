package com.atguigu01.mapreduce.outputFormat.copy;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/6 10 48
 */
public class CopyRecordWrite extends RecordWriter<LongWritable, Text> {
    FSDataOutputStream atguigu;
    FSDataOutputStream other;

    public void initialize(TaskAttemptContext job) throws IOException {
        String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    }

    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String line = value.toString() + "\n";
        if (line.contains("atguigu")){
            atguigu.write(line.getBytes());
        }else{
            other.write(line.getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}

package com.atguigu01.mapreduce.outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/6 10 14
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {
    FSDataOutputStream atguigu;
    FSDataOutputStream other;
//    FileOutputStream atguigu;
//    FileOutputStream other;

    public void initialize(TaskAttemptContext job) throws Exception {

        String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        atguigu = fileSystem.create(new Path(outdir + "/atguigu.log"));
        other = fileSystem.create(new Path(outdir + "/other.log"));
    }

    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String out = value.toString() + "\n";
        if (out.contains("atguigu")) {
            atguigu.write(out.getBytes());
        } else {
            other.write(out.getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}

package com.atguigu01.inputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {

    BytesWritable value = new BytesWritable();
    FileSplit split;
    Configuration configuration;
    private boolean isProcessed = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean isResult;
        if (!isProcessed) {
            byte[] buf = new byte[(int) split.getLength()];

            //获取系统文件
            FileSystem fs = FileSystem.get(configuration);
            Path path = split.getPath();
            FSDataInputStream fis = null;
            try {
                fis = fs.open(path);
                IOUtils.readFully(fis, buf, 0, buf.length);
                //设置输出流
                value.set(buf, 0, buf.length);
            } finally {
                IOUtils.closeStream(fis);
            }

            isProcessed = true;

            isResult = true;
        } else {
            isResult = false;
        }

        return isResult;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isProcessed ? 1:0;
    }

    @Override
    public void close() throws IOException {

    }
}

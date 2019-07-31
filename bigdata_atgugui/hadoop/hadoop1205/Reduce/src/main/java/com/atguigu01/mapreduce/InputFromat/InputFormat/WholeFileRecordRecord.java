package com.atguigu01.mapreduce.InputFromat.InputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/4 16 04
 */
public class WholeFileRecordRecord extends RecordReader<Text, BytesWritable> {

    private boolean notRead = true;

    private Text key = new Text();

    private BytesWritable value = new BytesWritable();

    FileSplit fs;
    FSDataInputStream inputStream;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        fs = (FileSplit) split;

        Path path = fs.getPath();
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        inputStream = fileSystem.open(path);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (notRead) {

            key.set(fs.getPath().toString());

            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);

            value.set(buf, 0, buf.length);
            notRead = false;
            return true;
        } else {
            return false;
        }


    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    @Override
    public void close() throws IOException {

    }
}

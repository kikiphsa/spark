package com.atguigu01.mapreduce.InputFromat;

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
 * Create by chenqinping on 2019/3/4 14 37
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    Text key = new Text();
    BytesWritable value = new BytesWritable();
    FileSplit fs;
    FSDataInputStream inputStream;

    private boolean notRead = true;

    /**
     * 初始haul方法
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        fs = (FileSplit) split;
        Path path = fs.getPath();
        Configuration configuration = context.getConfiguration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        inputStream = fileSystem.open(path);

    }

    /**
     * 读取kv
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
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

    /**
     * 记录读取器通过其数据的当前进度
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }

    @Override
    public void close() throws IOException {

    }
}

package com.atguigu01.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class MyFilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream atguigu =null;
    FSDataOutputStream other =null;
    public MyFilterRecordWriter(TaskAttemptContext job){
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
             atguigu = fs.create(new Path("D:\\atguigu.log"));
             other = fs.create(new Path("D:\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if (text.equals("atguigu")){
            atguigu.write(text.toString().getBytes());
        }else {
            other.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
// 关闭资源
        if (atguigu != null) {
            atguigu.close();
        }

        if (other != null) {
            other.close();
        }
    }
}

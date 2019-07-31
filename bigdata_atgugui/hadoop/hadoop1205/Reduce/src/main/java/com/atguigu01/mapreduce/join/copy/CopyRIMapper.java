package com.atguigu01.mapreduce.join.copy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/6 14 00
 */
public class CopyRIMapper extends Mapper<LongWritable, Text, CopyOrderBean, NullWritable> {
    String fileName;

    CopyOrderBean bean = new CopyOrderBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit fs = (FileSplit) context.getInputSplit();
        fileName = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        if (fileName.equals("order.txt")) {
            bean.setId(fields[0]);
            bean.setPid(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setPname("");
        } else {
            bean.setPid(fields[0]);
            bean.setPname(fields[1]);
            bean.setId("");
            bean.setAmount(0);
        }

        context.write(bean, NullWritable.get());


    }
}

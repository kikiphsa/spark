package com.atguigu01.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    TableBean v = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取文件名称
        FileSplit split = (FileSplit) context.getInputSplit();

        String name = split.getPath().getName();

        String line = value.toString();

        if (name.startsWith("order")) {
            String[] fields = line.split("\t");
            // 3 封装bean对象
            v.setOrder_id(fields[0]);
            v.setP_id(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setPname("");
            v.setFlag("0");

            k.set(fields[1]);
        } else {
            String[] fields = line.split("\t");

            v.setOrder_id("");
            v.setP_id(fields[0]);
            v.setAmount(0);
            v.setPname(fields[1]);
            v.setFlag("1");

            k.set(fields[0]);
        }
        context.write(k,v);

    }
}
